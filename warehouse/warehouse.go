package warehouse

/*
#cgo CFLAGS: -I.
#cgo LDFLAGS: -L. -L/usr/local/lib -L../lib -lrocksdb -ldisgorge -Wl,-rpath,./../lib
#include "../cpp/disgorge.h"
#include <stdlib.h>
*/
import "C"

import (
	"disgorge/api"
	"disgorge/config"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"go.uber.org/zap"
)

const success = "SUCCESS"
const interval int64 = 3600
const maxCount = 1000

func str2bytes(s string) (b []byte) {
	/* #nosec G103 */
	bh := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	/* #nosec G103 */
	sh := (*reflect.StringHeader)(unsafe.Pointer(&s))
	bh.Data = sh.Data
	bh.Cap = sh.Len
	bh.Len = sh.Len
	return b
}

func scan(query, start, end string, shard *api.Shard, status bool) []string {
	stat := prome.NewStat("warehouse.scan")
	defer stat.End()
	if shard == nil || shard.Status == api.ShardStatus_Finished ||
		shard.Status == api.ShardStatus_Error ||
		(!shard.HasMore) {
		zlog.LOG.Info("shard status check fail")
		return nil
	}

	shard.Status = api.ShardStatus_InProgress
	secondary := ""

	if status {
		ts := time.Now().Unix()
		idx := rand.Int63n(1000000)
		secondary = fmt.Sprintf("/tmp/%d-%d", ts, idx)
	}

	ins := C.disgorge_open(unsafe.Pointer(&str2bytes(shard.Path)[0]), C.ulonglong(len(shard.Path)),
		unsafe.Pointer(&str2bytes(secondary)[0]), C.ulonglong(len(secondary)))
	defer C.disgorge_close(ins)

	if ins == nil {
		stat.MarkErr()
		zlog.LOG.Error("fail to open rocksdb", zap.String("path", shard.Path))
		return nil
	}

	startKey := start

	if len(shard.Lastkey) > 0 {
		startKey = shard.Lastkey
	}

	resp := C.disgorge_scan(ins, unsafe.Pointer(&str2bytes(query)[0]), C.ulonglong(len(query)),
		unsafe.Pointer(&str2bytes(startKey)[0]), C.ulonglong(len(startKey)),
		unsafe.Pointer(&str2bytes(end)[0]), C.ulonglong(len(end)))
	defer C.disgorge_del_response(resp)
	ret := make([]string, 0, maxCount)

	if int(C.disgorge_response_more(resp)) == 1 {
		shard.HasMore = true
		shard.Lastkey = C.GoString(C.disgorge_response_lastkey(resp))
	} else {
		shard.HasMore = false
		shard.Lastkey = ""
		shard.Status = api.ShardStatus_Finished
	}

	size := uint64(C.disgorge_response_size(resp))

	for i := uint64(0); i < size; i++ {
		ptr := C.disgorge_response_value(resp, C.ulonglong(i))
		ret = append(ret, C.GoString(ptr))
	}
	return ret
}

func Query(req *api.Request) *api.Response {
	stat := prome.NewStat("warehouse.Query")
	defer stat.End()
	workdir := config.AppConf.WorkDir
	// build dict
	shardDict := make(map[string]*api.Shard, len(req.Shards))
	for i := 0; i < len(req.Shards); i++ {
		shardDict[req.Shards[i].Path] = req.Shards[i]
	}

	startTs := req.Start - 10
	endTs := req.End + 10
	shards := make([]*api.Shard, 0)
	status := make([]bool, 0)

	// list dirs
	files, err := os.ReadDir(workdir)
	if err != nil {
		stat.MarkErr()
		zlog.LOG.Error("list dir error", zap.String("workdir", workdir))
		return nil
	}

	// filter and list the shards and open status
	for i := 0; i < len(files); i++ {
		if !files[i].IsDir() {
			continue
		}

		ip := files[i].Name()
		ipPath := path.Join(workdir, ip)
		subFiles, err := os.ReadDir(ipPath)
		if err != nil {
			continue
		}

		for j := 0; j < len(subFiles); j++ {
			if !subFiles[j].IsDir() {
				continue
			}

			ts, err := strconv.ParseInt(subFiles[j].Name(), 10, 64)
			if err != nil {
				continue
			}

			if !((ts <= startTs && startTs <= ts+interval) ||
				(ts <= endTs && endTs <= ts+interval)) {
				continue
			}

			shardPath := path.Join(ipPath, subFiles[j].Name())
			shard, ok := shardDict[shardPath]
			if !ok {
				shard = &api.Shard{
					Status:  api.ShardStatus_NotStarted,
					Path:    shardPath,
					HasMore: true,
					Lastkey: "",
				}
			}
			shards = append(shards, shard)
			if _, err := os.Stat(path.Join(ipPath, subFiles[j].Name(), success)); os.IsNotExist(err) {
				status = append(status, false)
			} else {
				status = append(status, true)
			}
		}
	}

	// do query
	var startPrefix, endPrefix string
	if req.UserId != "" {
		startPrefix = fmt.Sprintf("%s|%d", req.UserId, startTs)
		endPrefix = fmt.Sprintf("%s|%d", req.UserId, endTs)
	}

	resp := &api.Response{
		Data:   make([]*api.Data, len(shards)),
		Shards: shards,
		Code:   200,
	}

	count := 0

	for i := 0; i < len(shards); i++ {
		if shards[i].Status == api.ShardStatus_Error ||
			shards[i].Status == api.ShardStatus_Finished {
			continue
		}
		resp.Data[i] = &api.Data{
			Items: make([]string, 0, maxCount),
		}

		items := scan(req.Query, startPrefix, endPrefix, shards[i], status[i])
		resp.Data[i].Items = append(resp.Data[i].Items, items...)
		count += len(items)
		if count >= maxCount {
			break
		}
	}
	stat.SetCounter(count)
	return resp
}

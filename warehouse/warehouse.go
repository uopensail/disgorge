package warehouse

/*
#cgo darwin,amd64 pkg-config: ${SRCDIR}/../third/disgorge-darwin-amd64.pc
#cgo darwin,arm64 pkg-config: ${SRCDIR}/../third/disgorge-darwin-arm64.pc
#cgo linux,amd64 pkg-config:  ${SRCDIR}/../third/disgorge-linux-amd64.pc
#include <stdlib.h>
#include "disgorge.h"
#include <string.h>
#include <stdio.h>
*/
import "C"

import (
	"disgorge/config"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"path"
	"reflect"
	"strconv"
	"time"
	"unsafe"

	"github.com/uopensail/swallow-idl/proto/disgorgeapi"
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

func FindDbPath(workdir string, startTs, endTs int64) ([]string, []bool) {

	// list dirs
	files, err := os.ReadDir(workdir)
	if err != nil {

		zlog.LOG.Error("list dir error", zap.String("workdir", workdir))
		return nil, nil
	}
	var paths []string
	var asSecondary []bool
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

			paths = append(paths, shardPath)
			if _, err := os.Stat(path.Join(ipPath, subFiles[j].Name(), success)); os.IsNotExist(err) {
				asSecondary = append(asSecondary, true)
			} else {
				asSecondary = append(asSecondary, false)
			}
		}
	}
	return paths, asSecondary
}
func FetchAll(filterQL, startPrefix, endPrefix string, paths []string, asSecondarys []bool) []string {
	var vvs []string
	for i := 0; i < len(paths); i++ {
		vv, _ := FetchSingleAll("", startPrefix, endPrefix, paths[i], asSecondarys[i])
		vvs = append(vvs, vv...)
	}
	return vvs
}
func FetchSingleAll(filterQL, startPrefix, endPrefix, path string, asSecondary bool) ([]string, error) {
	secondary := ""
	if asSecondary {
		ts := time.Now().UnixNano()
		idx := rand.Int63n(1000000)
		secondary = fmt.Sprintf("/tmp/%d-%d", ts, idx)
	}
	dirCString := C.CString(path)
	defer C.free(unsafe.Pointer(dirCString))

	secondaryCString := C.CString(secondary)
	defer C.free(unsafe.Pointer(secondaryCString))
	ins := C.disgorge_open(dirCString, secondaryCString)

	defer C.disgorge_close(ins)

	if ins == nil {

		zlog.LOG.Error("fail to open rocksdb", zap.String("path", path))
		return nil, errors.New("fail to open rocksdb")
	}

	startKey := startPrefix
	var filterPointer unsafe.Pointer
	if len(filterQL) > 0 {
		filterPointer = unsafe.Pointer(&str2bytes(filterQL)[0])
	}
	resp := C.disgorge_scan(ins, filterPointer, C.ulonglong(len(filterQL)),
		unsafe.Pointer(&str2bytes(startKey)[0]), C.ulonglong(len(startKey)),
		unsafe.Pointer(&str2bytes(endPrefix)[0]), C.ulonglong(len(endPrefix)), C.longlong(-1))

	defer C.disgorge_del_response(resp)
	ret := make([]string, 0, maxCount)

	size := uint64(C.disgorge_response_size(resp))

	for i := uint64(0); i < size; i++ {
		ptr := C.disgorge_response_value(resp, C.ulonglong(i))
		ret = append(ret, C.GoString(ptr))
	}
	return ret, nil
}

func scan(filterQL, start, end string, shard *disgorgeapi.Shard, status bool) []string {
	stat := prome.NewStat("warehouse.scan")
	defer stat.End()
	if shard == nil || shard.Status == disgorgeapi.ShardStatus_Finished ||
		shard.Status == disgorgeapi.ShardStatus_Error ||
		(!shard.HasMore) {
		zlog.LOG.Info("shard status check fail")
		return nil
	}

	shard.Status = disgorgeapi.ShardStatus_InProgress
	secondary := ""

	if status {
		ts := time.Now().UnixNano()
		idx := rand.Int63n(1000000)
		secondary = fmt.Sprintf("/tmp/%d-%d", ts, idx)
	}

	dirCString := C.CString(shard.Path)
	defer C.free(unsafe.Pointer(dirCString))

	secondaryCString := C.CString(secondary)
	defer C.free(unsafe.Pointer(secondaryCString))
	ins := C.disgorge_open(dirCString, secondaryCString)

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
	var filterPointer unsafe.Pointer
	if len(filterQL) > 0 {
		filterPointer = unsafe.Pointer(&str2bytes(filterQL)[0])
	}
	resp := C.disgorge_scan(ins, filterPointer, C.ulonglong(len(filterQL)),
		unsafe.Pointer(&str2bytes(startKey)[0]), C.ulonglong(len(startKey)),
		unsafe.Pointer(&str2bytes(end)[0]), C.ulonglong(len(end)), C.longlong(30))
	defer C.disgorge_del_response(resp)
	ret := make([]string, 0, maxCount)

	if int(C.disgorge_response_more(resp)) == 1 {
		shard.HasMore = true
		shard.Lastkey = C.GoString(C.disgorge_response_lastkey(resp))
	} else {
		shard.HasMore = false
		shard.Lastkey = ""
		shard.Status = disgorgeapi.ShardStatus_Finished
	}

	size := uint64(C.disgorge_response_size(resp))

	for i := uint64(0); i < size; i++ {
		ptr := C.disgorge_response_value(resp, C.ulonglong(i))
		ret = append(ret, C.GoString(ptr))
	}
	return ret
}

func Query(req *disgorgeapi.Request) *disgorgeapi.Response {
	stat := prome.NewStat("warehouse.Query")
	defer stat.End()

	workdir := config.AppConfigInstance.WorkDir
	// build dict
	shardDict := make(map[string]*disgorgeapi.Shard, len(req.Shards))
	for i := 0; i < len(req.Shards); i++ {
		shardDict[req.Shards[i].Path] = req.Shards[i]
	}

	startTs := req.Start - 10
	endTs := req.End + 10
	shards := make([]*disgorgeapi.Shard, 0)
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
				shard = &disgorgeapi.Shard{
					Status:  disgorgeapi.ShardStatus_NotStarted,
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
		startPrefix = fmt.Sprintf("%s|%d", req.UserId, req.Start)
		endPrefix = fmt.Sprintf("%s|%d", req.UserId, req.End+1)
	}

	resp := &disgorgeapi.Response{
		Data:   make([]*disgorgeapi.Data, len(shards)),
		Shards: shards,
		Code:   200,
	}

	count := 0

	for i := 0; i < len(shards); i++ {
		if shards[i].Status == disgorgeapi.ShardStatus_Error ||
			shards[i].Status == disgorgeapi.ShardStatus_Finished {
			continue
		}
		resp.Data[i] = &disgorgeapi.Data{
			Items: make([]string, 0, maxCount),
		}

		items := scan(req.Query, startPrefix, endPrefix, shards[i], status[i])
		resp.Data[i].Items = append(resp.Data[i].Items, items...)
		count += len(items)
		if maxCount >= 0 && count >= maxCount {
			break
		}
	}
	stat.SetCounter(count)
	return resp
}

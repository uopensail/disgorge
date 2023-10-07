package services

import (
	"disgorge/config"
	"disgorge/warehouse"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/bytedance/sonic"
	"github.com/gin-gonic/gin"
	ui "github.com/jaegertracing/jaeger/model/json"
	"github.com/uopensail/ulib/utils"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type structuredError struct {
	Code    int        `json:"code,omitempty"`
	Msg     string     `json:"msg"`
	TraceID ui.TraceID `json:"traceID,omitempty"`
}
type structuredResponse struct {
	Data   interface{}       `json:"data"`
	Total  int               `json:"total"`
	Limit  int               `json:"limit"`
	Offset int               `json:"offset"`
	Errors []structuredError `json:"errors"`
}

type SpanCodeStatus int8

const (
	SpanCodeUNSET SpanCodeStatus = 0
	SpanCodeOK    SpanCodeStatus = 1
	SpanCodeERROR SpanCodeStatus = 2
)

type SpanKind int8

const (
	SpanKindUNSPECIFIED SpanKind = 0
	SpanKindNTERNAL     SpanKind = 1
	SpanKindSERVER      SpanKind = 2
	SpanKindCLIENT      SpanKind = 3
	SpanKindPRODUCER    SpanKind = 4
	SpanKindCONSUMER    SpanKind = 5
)

type Status struct {
	Code    SpanCodeStatus `json:"code"`
	Message string         `json:"msg"`
}
type SpanBase struct {
	PrimaryKey   string   `json:"primary_key"`
	Name         string   `json:"name"`
	TraceID      string   `json:"trace_id"`
	SpanID       string   `json:"span_id"`
	ParentSpanID string   `json:"parent_span_id"`
	BeginTime    int64    `json:"begin_time"`
	EndTime      int64    `json:"end_time"`
	Status       Status   `json:"status"`
	Kind         SpanKind `json:"kind"`
	DropLog      bool     `json:"drop_detail"`
}

func (s *SpanBase) MarshalSimple() ([]byte, error) {
	return sonic.Marshal(&(s))
}

type SpanModel struct {
	LibName         string `json:"lib_name"`
	SpanBase        `json:",inline"`
	SpanModelDetail `json:",inline"`
}

type SpanModelDetail struct {
	Attributes map[string]string      `json:"attr"`
	Events     map[string]interface{} `json:"events"`
}

func eventToJaegerFields(events map[string]interface{}) []ui.KeyValue {
	fields := make([]ui.KeyValue, 0, len(events))
	for k, v := range events {
		kv := ui.KeyValue{
			Key: k,
		}
		switch v.(type) {
		case string:
			kv.Type = ui.StringType
			kv.Value = v
		case bool:
			kv.Type = ui.BoolType
			kv.Value = v
		case int8, int16, int32, int64:
			kv.Type = ui.Int64Type
			kv.Value = v
		case uint8, uint16, uint32, uint64:
			kv.Type = ui.Int64Type
			kv.Value = v
		case float32, float64:
			kv.Type = ui.Float64Type
			kv.Value = v
		default:
			kv.Type = ui.BinaryType
			kv.Value, _ = json.Marshal(v)
		}
		fields = append(fields, kv)
	}
	return fields
}
func convertToJaeger(values []string) []*ui.Trace {
	if len(values) > 0 {
		spans := make([]SpanModel, 0, len(values))
		for _, v := range values {
			var span SpanModel
			err := json.Unmarshal([]byte(v), &span)
			if err == nil {
				spans = append(spans, span)
			}
		}

		type midResult struct {
			*ui.Trace
			service       int
			servicesNames map[string]string
		}

		ret := make(map[string]*midResult)
		if len(spans) > 0 {

			for i := 0; i < len(spans); i++ {
				span := &spans[i]
				var midR *midResult
				if _, ok := ret[span.TraceID]; !ok {
					midR = &midResult{}
					singleRet := ui.Trace{
						TraceID:   ui.TraceID(span.TraceID),
						Processes: make(map[ui.ProcessID]ui.Process),
					}
					singleRet.Spans = make([]ui.Span, 0, len(spans))
					midR.servicesNames = make(map[string]string, len(spans))
					midR.Trace = &singleRet
					ret[span.TraceID] = midR
				} else {
					midR = ret[span.TraceID]
				}

				if _, ok := midR.servicesNames[span.LibName]; !ok {
					midR.service++
					midR.servicesNames[span.LibName] = fmt.Sprintf("p%d", midR.service)
				}
				uiSpan := ui.Span{
					TraceID:       ui.TraceID(span.TraceID),
					SpanID:        ui.SpanID(span.SpanID),
					ParentSpanID:  ui.SpanID(span.ParentSpanID),
					OperationName: span.Name,
					StartTime:     uint64(span.BeginTime) / 1000,
					Duration:      uint64(span.EndTime-span.BeginTime) / 1000,
					ProcessID:     ui.ProcessID(fmt.Sprintf("p%d", midR.service)),
				}

				if len(span.ParentSpanID) > 0 {
					uiSpan.References = make([]ui.Reference, 1)
					uiSpan.References[0] = ui.Reference{
						RefType: ui.ChildOf,
						TraceID: ui.TraceID(span.TraceID),
						SpanID:  ui.SpanID(span.ParentSpanID),
					}
				}
				if len(span.Events) > 0 {
					uilog := ui.Log{
						Timestamp: uint64(span.BeginTime) / 1000,
					}
					uilog.Fields = eventToJaegerFields(span.Events)
					uiSpan.Logs = []ui.Log{uilog}
				}

				midR.Trace.Spans = append(midR.Trace.Spans, uiSpan)

			}
			for _, singleRet := range ret {
				for k, v := range singleRet.servicesNames {
					singleRet.Processes[ui.ProcessID(v)] = ui.Process{
						ServiceName: k,
					}
				}
			}
			retArr := make([]*ui.Trace, 0, len(ret))
			for _, singleRet := range ret {
				retArr = append(retArr, singleRet.Trace)
			}

			return retArr
		}
	}
	return nil
}

func (srv *Services) getTraceHandler(gCtx *gin.Context) {
	traceID := gCtx.Param("trace_id")

	startMircs := utils.String2Int64(gCtx.Query("start"))
	endMircs := utils.String2Int64(gCtx.Query("end"))
	if endMircs <= 0 {
		endMircs = math.MaxInt64
	}

	paths, asSecondarys := warehouse.FindDbPath(config.AppConfigInstance.WorkDir, startMircs/1e6, endMircs/1e6)
	prefix := traceID
	vvs := warehouse.FetchAll("", prefix, prefix+string(byte(0xff)), paths, asSecondarys)

	uiTraces := convertToJaeger(vvs)
	gCtx.JSON(200, structuredResponse{
		Data: uiTraces,
	})
	return
}
func (srv *Services) searchHandler(gCtx *gin.Context) {
	userID := gCtx.Query("userId")
	startMircs := utils.String2Int64(gCtx.Query("start"))
	endMircs := utils.String2Int64(gCtx.Query("end"))
	data := srv.search(userID, startMircs*1e3, endMircs*1e3)
	if len(data) <= 0 {
		gCtx.JSON(500, errors.New("not found"))
		return
	}
	gCtx.JSON(200, structuredResponse{
		Data: data,
	})
	return
}

func (srv *Services) search(userID string, beginTime int64, endtime int64) []*ui.Trace {
	paths, asSecondarys := warehouse.FindDbPath(config.AppConfigInstance.WorkDir, int64(beginTime)/1e9, int64(endtime)/1e9)

	var startPrefix, endPrefix string
	if len(userID) > 0 {
		//use userID search
		startPrefix = "traceuidindex|" + userID
		startPrefix = startPrefix + "|" + strconv.FormatInt(beginTime, 10)

		endPrefix = "traceuidindex|" + userID
		endPrefix = endPrefix + "|" + strconv.FormatInt(endtime, 10)
	} else {
		startPrefix = "tracetimeindex|" + strconv.FormatInt(beginTime, 10)
		endPrefix = "tracetimeindex|" + strconv.FormatInt(endtime, 10)
	}

	traceIds := warehouse.FetchAll("", startPrefix, endPrefix, paths, asSecondarys)

	//二次查找,通过traceID
	spanDatas := make([]string, 0, len(paths))
	for i := 0; i < len(traceIds); i++ {
		spvs := warehouse.FetchAll("", traceIds[i], traceIds[i]+string(byte(0xff)), paths, asSecondarys)

		if len(spvs) > 0 {
			spanDatas = append(spanDatas, spvs...)
		}
	}

	return convertToJaeger(spanDatas)

}
func (srv *Services) getServicesHandler(gCtx *gin.Context) (err error) {
	services := []string{"test"}
	gCtx.JSON(200, structuredResponse{
		Data:  services,
		Total: len(services),
	})
	return
}
func (srv *Services) getOperations(gCtx *gin.Context) (err error) {
	return status.Errorf(codes.Unimplemented, "method GetOperations not implemented")
}

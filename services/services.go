package services

import (
	"context"
	"disgorge/warehouse"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/uopensail/sunmao/utils"
	"github.com/uopensail/swallow-idl/proto/disgorgeapi"
	"github.com/uopensail/ulib/prome"
	etcdclient "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var __GITHASH__ = ""

type Services struct {
	disgorgeapi.UnimplementedDisgorgeServiceServer
}

func NewServices() *Services {
	return &Services{}
}
func (srv *Services) Init(configFolder string, etcdName string, etcdCli *etcdclient.Client, reg utils.Register) {

}
func (srv *Services) RegisterGrpc(grpcS *grpc.Server) {
	disgorgeapi.RegisterDisgorgeServiceServer(grpcS, srv)

}

func (srv *Services) RegisterGinRouter(ginEngine *gin.Engine) {
	ginEngine.POST("/query", srv.QueryHandler)
	ginEngine.GET("/trace/:trace_id", srv.getTraceHandler)
	ginEngine.GET("/search", srv.searchHandler)
}
func (srv *Services) Close() {

}

func (srv *Services) Query(ctx context.Context, in *disgorgeapi.Request) (*disgorgeapi.Response, error) {
	stat := prome.NewStat("App.Query")
	defer stat.End()
	response := warehouse.Query(in)
	return response, nil
}

func (srv *Services) QueryHandler(gCtx *gin.Context) {
	stat := prome.NewStat("App.QueryEchoHandler")
	defer stat.End()
	request := &disgorgeapi.Request{}
	if err := gCtx.ShouldBind(request); err != nil {
		stat.MarkErr()
		return
	}
	response, err := srv.Query(context.Background(), request)
	if err != nil {
		stat.MarkErr()
		return
	}
	gCtx.JSON(http.StatusOK, response)
}

func (srv *Services) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (srv *Services) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
	return nil
}

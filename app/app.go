package app

import (
	"context"
	"disgorge/api"
	"disgorge/warehouse"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/uopensail/ulib/prome"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var __GITHASH__ = ""

type App struct{}

func NewApp() *App {
	return &App{}
}

func (app *App) Close() {}

func (app *App) GRPCAPIRegister(s *grpc.Server) {
	api.RegisterDisgorgeServiceServer(s, app)
}

func (app *App) RegisterGinRouter(ginEngine *gin.Engine) {
	ginEngine.POST("/query", app.QueryHandler)
	ginEngine.GET("/", app.PingHandler)
	ginEngine.GET("/version", app.VersionHandler)
}

func (app *App) Query(ctx context.Context, in *api.Request) (*api.Response, error) {
	stat := prome.NewStat("App.Query")
	defer stat.End()
	response := warehouse.Query(in)
	return response, nil
}

func (app *App) QueryHandler(gCtx *gin.Context) {
	stat := prome.NewStat("App.QueryEchoHandler")
	defer stat.End()
	request := &api.Request{}
	if err := gCtx.ShouldBind(request); err != nil {
		stat.MarkErr()
		return
	}
	response, err := app.Query(context.Background(), request)
	if err != nil {
		stat.MarkErr()
		return
	}
	gCtx.JSON(http.StatusOK, response)
}

func (app *App) PingHandler(gCtx *gin.Context) {
	gCtx.String(200, "PONG")
}

func (app *App) VersionHandler(gCtx *gin.Context) {
	gCtx.String(200, __GITHASH__)
}

func (app *App) Check(ctx context.Context, req *grpc_health_v1.HealthCheckRequest) (*grpc_health_v1.HealthCheckResponse, error) {
	return &grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_NOT_SERVING,
	}, nil
}

func (app *App) Watch(req *grpc_health_v1.HealthCheckRequest, server grpc_health_v1.Health_WatchServer) error {
	server.Send(&grpc_health_v1.HealthCheckResponse{
		Status: grpc_health_v1.HealthCheckResponse_SERVING,
	})
	return nil
}

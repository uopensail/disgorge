package app

import (
	"context"
	"disgorge/api"
	"disgorge/warehouse"

	"github.com/labstack/echo/v4"
	"github.com/uopensail/ulib/prome"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var __GITHASH__ = ""

type App struct{}

func NewApp() *App {
	return &App{}
}

func (app *App) GRPCAPIRegister(s *grpc.Server) {
	api.RegisterDisgorgeServiceServer(s, app)
}

func (app *App) EchoAPIRegister(e *echo.Echo) {
	e.POST("/query", app.QueryEchoHandler)
	e.POST("/", app.PingEchoHandler)
	e.POST("/version", app.VersionEchoHandler)
}

func (app *App) Query(ctx context.Context, in *api.Request) (*api.Response, error) {
	stat := prome.NewStat("App.Query")
	defer stat.End()
	response := warehouse.Query(in)
	return response, nil
}

func (app *App) QueryEchoHandler(c echo.Context) (err error) {
	stat := prome.NewStat("App.QueryEchoHandler")
	defer stat.End()
	request := &api.Request{}
	if err = c.Bind(request); err != nil {
		stat.MarkErr()
		return err
	}
	response, err := app.Query(context.Background(), request)
	if err != nil {
		stat.MarkErr()
		return err
	}
	return c.JSON(200, response)
}

func (app *App) PingEchoHandler(c echo.Context) (err error) {
	return c.JSON(200, "OK")
}

func (app *App) VersionEchoHandler(c echo.Context) (err error) {
	return c.JSON(200, __GITHASH__)
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

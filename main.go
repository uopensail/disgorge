package main

import (
	"disgorge/app"
	"disgorge/config"
	"flag"
	"fmt"
	"net"

	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/uopensail/ulib/prome"
	"github.com/uopensail/ulib/zlog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/health/grpc_health_v1"
)

func runGRPC() {
	go func() {
		app := app.NewApp()
		grpcServer := grpc.NewServer()
		//添加监控检测服务
		grpc_health_v1.RegisterHealthServer(grpcServer, app)
		app.GRPCAPIRegister(grpcServer)
		listener, err := net.Listen("tcp", fmt.Sprintf(":%d", config.AppConf.GRPCPort))
		if err != nil {
			zlog.SLOG.Fatal(err)
			panic(err)
		}

		err = grpcServer.Serve(listener)
		panic(err)
	}()
}

func runProme() {
	go func() {
		exporter := prome.NewExporter(config.AppConf.ProjectName)
		err := exporter.Start(config.AppConf.PromePort)
		if err != nil {
			zlog.SLOG.Fatal(err)
			panic(err)
		}
	}()
}

func runHttp() {
	e := echo.New()
	app := app.NewApp()
	e.Use(middleware.Recover())
	app.EchoAPIRegister(e)
	e.Logger.Fatal(e.Start(fmt.Sprintf(":%d", config.AppConf.HTTPPort)))
}

func main() {
	configPath := flag.String("config", "conf/config.toml", "path of configure")
	flag.Parse()

	config.AppConf.Init(*configPath)
	zlog.InitLogger(config.AppConf.ProjectName,
		config.AppConf.Debug,
		config.AppConf.LogDir)

	runGRPC()
	runProme()
	runHttp()
}

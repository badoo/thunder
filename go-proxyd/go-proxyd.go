package main

import (
	"badoo/_packages/log"
	"badoo/_packages/service"
	"badoo/_packages/service/config"
	"github.com/badoo/thunder/go-proxyd/proto"
	"syscall"
)

type FullConfig struct {
	badoo_config.ServiceConfig
	badoo_phproxyd.PhproxydConfig
}

var config = FullConfig{}

var ports = []service.Port{
	service.GpbPort("phproxyd-gpb", &gpb_ctx, badoo_phproxyd.Gpbrpc),
	service.JsonPort("phproxyd-gpb/json", &gpb_ctx, badoo_phproxyd.Gpbrpc),
}

func main() {

	service.Initialize("conf/phproxyd.conf", &config)

	q, w, e := syscall.Syscall(syscall.SYS_GETPRIORITY, 1, 0, 0)
	log.Debug("GetPriority: %v %v %v", q, w, e)

	sh = newShardedCache(32)

	service.EventLoop(ports)
}

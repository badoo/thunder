package main

import (
	"badoo/_packages/gpbrpc"
	"badoo/_packages/log"
	"badoo/_packages/service"
	"github.com/badoo/thunder/common"
	"github.com/badoo/thunder/db"
	"github.com/badoo/thunder/jobgen"
	"github.com/badoo/thunder/proto"
	"net/http"

	"github.com/gogo/protobuf/proto"
)

var config = common.FullConfig{}

type GPBContext struct {
}

func (ctx *GPBContext) RequestAddJobs(rctx gpbrpc.RequestT, request *thunder.RequestAddJobs) gpbrpc.ResultT {
	ids, err := jobgen.APIAcceptTTJobs(request.Jobs)

	if err != nil {
		return thunder.Gpbrpc.ErrorGeneric(err.Error())
	}

	return gpbrpc.Result(&thunder.ResponseJobIds{JobId: ids})
}

func (ctx *GPBContext) RequestUpdateStatus(rctx gpbrpc.RequestT, request *thunder.RequestUpdateStatus) gpbrpc.ResultT {
	err := jobgen.APIUpdateRunStatus(request.GetHostname(), request.GetRunId(), request.GetPrevStatus(), request.GetStatus())
	if err != nil {
		return thunder.Gpbrpc.ErrorGeneric(err.Error())
	}

	return thunder.Gpbrpc.OK()
}

func (ctx *GPBContext) RequestLogFinish(rctx gpbrpc.RequestT, request *thunder.RequestLogFinish) gpbrpc.ResultT {
	info, err := jobgen.APILogFinish(request.GetHostname(), request.GetRunId(), request.GetPrevStatus(), request.GetSuccess())
	if err != nil {
		return thunder.Gpbrpc.ErrorGeneric(err.Error())
	}

	return gpbrpc.Result(&thunder.ResponseRunInfo{Info: proto.String(info)})
}

func checkSelect42() {
	rows, err := db.Query("SELECT 42")
	if err != nil {
		log.Fatal("Could not SELECT 42 from DB: " + err.Error())
	}

	defer rows.Close()

	var intRes int

	for rows.Next() {
		if err := rows.Scan(&intRes); err != nil {
			log.Fatal("Could not fetch results of SELECT 42: " + err.Error())
		}

		if intRes != 42 {
			log.Fatal("SELECT 42 did not return '42', perhaps something is broken in either client or server")
		}

		// TODO: maybe check that only one exists?
	}
}

func setupAndCheckMysql() {
	mysqlConf := config.GetMysql()

	log.Info("Connecting to MySQL")

	if err := db.Setup(mysqlConf.GetDsn(), mysqlConf.GetMaxConnections()); err != nil {
		log.Fatal("Could not estabilish db connection: " + err.Error())
	}

	checkSelect42()

	log.Info("Connection successfull")
}

func main() {
	var gpb = GPBContext{}

	var ports = []service.Port{
		service.GpbPort("thunder-gpb", &gpb, thunder.Gpbrpc),
		service.JsonPort("thunder-gpb/json", &gpb, thunder.Gpbrpc),
	}

	service.Initialize("conf/thunder.conf", &config)

	setupAndCheckMysql()

	http.HandleFunc("/debug/thunder/", debugPage)
	http.HandleFunc("/debug/thunder/jobs", jobsDebugPage)

	common.Setup()
	jobgen.Setup(config)
	go jobgen.WriteLogsThread(config.GetLauncher().GetRqhLogFile())
	go jobgen.GenerateJobsCycle()

	service.EventLoop(ports)
}

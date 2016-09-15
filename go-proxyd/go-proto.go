package main

import (
	"badoo/_packages/gpbrpc"
	"github.com/badoo/thunder/go-proxyd/proto"
	"github.com/badoo/thunder/go-proxyd/vproc"
	"github.com/gogo/protobuf/proto"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
)

type GPBContext struct {
}

var gpb_ctx = GPBContext{}

func (service *GPBContext) RequestRrd(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestRrd) gpbrpc.ResultT {
	return badoo_phproxyd.Gpbrpc.ErrorGeneric("not implemented")
}

func (service *GPBContext) RequestPing(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestPing) gpbrpc.ResultT {
	status := badoo_phproxyd.PhproxydStatusT_STATUS_OK
	response := &badoo_phproxyd.ResponsePing{
		Status: &status,
	}
	return gpbrpc.Result(response)
}

func (service *GPBContext) RequestStats(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestStats) gpbrpc.ResultT {
	return badoo_phproxyd.Gpbrpc.ErrorGeneric("not implemented. Yaaaaaa")
}

func StdoutFile(store badoo_phproxyd.StoreT, id uint64) string {
	if store == badoo_phproxyd.StoreT_MEMORY {
		return fmt.Sprintf(config.GetScripts().GetMemStdoutTemplate(), id)
	}
	return fmt.Sprintf(config.GetScripts().GetFileStdoutTemplate(), id)
}

func StderrFile(store badoo_phproxyd.StoreT, id uint64) string {
	if store == badoo_phproxyd.StoreT_MEMORY {
		return fmt.Sprintf(config.GetScripts().GetMemStderrTemplate(), id)
	}
	return fmt.Sprintf(config.GetScripts().GetFileStderrTemplate(), id)
}

func (service *GPBContext) RequestRun(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestRun) gpbrpc.ResultT {
	sh.Lock(request.GetHash())
	pe, exists := sh.Get(request.GetHash())
	if exists == true {
		if pe.(*vproc.Vproc).Ps == nil {
			if request.GetForce() > 0 {
				//pe.(*vproc.Vproc).Kill()
				os.Remove(StdoutFile(request.GetStore(), pe.(*vproc.Vproc).Id))
				os.Remove(StderrFile(request.GetStore(), pe.(*vproc.Vproc).Id))
			} else {
				sh.Unlock(request.GetHash())
				return badoo_phproxyd.Gpbrpc.ErrorAlreadyRunning()
			}
		}
		sh.Delete(pe.(*vproc.Vproc).Id)
	}
	sh.Unlock(request.GetHash())

	p, err := vproc.NewProcess(request.GetHash(), config.GetScripts().GetPhpBin(), request.GetScript(), request.GetParams(),
		StdoutFile(request.GetStore(), request.GetHash()),
		StderrFile(request.GetStore(), request.GetHash()))
	if err != nil {
		return badoo_phproxyd.Gpbrpc.ErrorRunFailed(err)
	}

	p.SendResponse = request.GetStore() == badoo_phproxyd.StoreT_MEMORY

	sh.Lock(request.GetHash())
	sh.Set(request.GetHash(), p)
	sh.Unlock(request.GetHash())

	err = p.Cmd.Start()
	if err != nil {
		return badoo_phproxyd.Gpbrpc.ErrorRunFailed(err)
	}

	go processWait(p)

	return badoo_phproxyd.Gpbrpc.OK()
}

func (service *GPBContext) RequestCheck(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestCheck) gpbrpc.ResultT {
	sh.Lock(request.GetHash())

	pe, exists := sh.Get(request.GetHash())
	if exists == false {
		sh.Unlock(request.GetHash())
		return badoo_phproxyd.Gpbrpc.ErrorNotFound()
	}

	if pe.(*vproc.Vproc).Ps == nil {
		sh.Unlock(request.GetHash())
		return badoo_phproxyd.Gpbrpc.ErrorAlreadyRunning()
	}

	// TODO: what the fuck, badoo_phproxyd.Errno_ERRNO_SUCCESS_FINISHED, or badoo_phproxyd.Gpbrpc.ErrorSuccessFinished(...) ?
	errcode := -badoo_phproxyd.Errno_value["ERRNO_SUCCESS_FINISHED"]
	sysusage := pe.(*vproc.Vproc).Ps.SysUsage()
	waitstatus := pe.(*vproc.Vproc).Ps.Sys().(syscall.WaitStatus).ExitStatus()
	sh.Unlock(request.GetHash())

	var mm badoo_phproxyd.StoreT
	if pe.(*vproc.Vproc).SendResponse == true {
		mm = badoo_phproxyd.StoreT_MEMORY
	} else {
		mm = badoo_phproxyd.StoreT_FILES
	}

	var data []byte
	if pe.(*vproc.Vproc).SendResponse {
		data, _ = ioutil.ReadFile(StdoutFile(mm, request.GetHash()))
		os.Remove(StdoutFile(mm, request.GetHash()))
		os.Remove(StderrFile(mm, request.GetHash()))
	}

	response := &badoo_phproxyd.ResponseCheck{
		ErrorCode:   &errcode,
		Response:    proto.String(string(data)),
		UtimeTvSec:  proto.Uint64(uint64(sysusage.(*syscall.Rusage).Utime.Sec)),
		UtimeTvUsec: proto.Uint64(uint64(sysusage.(*syscall.Rusage).Utime.Usec)),
		StimeTvSec:  proto.Uint64(uint64(sysusage.(*syscall.Rusage).Stime.Sec)),
		StimeTvUsec: proto.Uint64(uint64(sysusage.(*syscall.Rusage).Stime.Usec)),
		Retcode:     proto.Int32(int32(waitstatus)),
	}

	return gpbrpc.Result(response)
}

func (service *GPBContext) RequestFree(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestFree) gpbrpc.ResultT {
	sh.Lock(request.GetHash())
	defer sh.Unlock(request.GetHash())

	pe, exists := sh.Get(request.GetHash())
	if exists == false {
		return badoo_phproxyd.Gpbrpc.ErrorNotFound()
	}

	if pe.(*vproc.Vproc).Ps == nil {
		return badoo_phproxyd.Gpbrpc.ErrorAlreadyRunning()
	}

	sh.Delete(request.GetHash())

	return badoo_phproxyd.Gpbrpc.OK()
}

func (service *GPBContext) RequestTerminate(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestTerminate) gpbrpc.ResultT {
	sh.Lock(request.GetHash())
	defer sh.Unlock(request.GetHash())

	pe, exists := sh.Get(request.GetHash())
	if exists == false {
		return badoo_phproxyd.Gpbrpc.ErrorNotFound()
	}

	if pe.(*vproc.Vproc).Ps == nil {
		pe.(*vproc.Vproc).Kill()
	}

	return badoo_phproxyd.Gpbrpc.OK()
}

func (service *GPBContext) RequestSoftRun(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestSoftRun) gpbrpc.ResultT {
	return badoo_phproxyd.Gpbrpc.ErrorGeneric("not implemented")
}

func (service *GPBContext) RequestSoftCheck(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestSoftCheck) gpbrpc.ResultT {
	return badoo_phproxyd.Gpbrpc.ErrorGeneric("not implemented")
}

func (service *GPBContext) RequestSoftFree(conn gpbrpc.RequestT, request *badoo_phproxyd.RequestSoftFree) gpbrpc.ResultT {
	return badoo_phproxyd.Gpbrpc.ErrorGeneric("not implemented")
}

func processWait(p *vproc.Vproc) {
	ps, _ := p.Wait()
	sh.Lock(p.Id)
	p.Ps = ps
	sh.Unlock(p.Id)
}

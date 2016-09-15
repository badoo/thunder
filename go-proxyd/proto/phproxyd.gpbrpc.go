// Code generated by protoc-gen-gpbrpc-go.
// source: phproxyd.proto
// DO NOT EDIT!

package badoo_phproxyd

import "github.com/gogo/protobuf/proto"
import "badoo/_packages/gpbrpc"
import "fmt"

type GpbrpcType struct {
}

var Gpbrpc GpbrpcType

var RequestMsgid_gpbrpc_name = map[uint32]string{
	1:  "request_rrd",
	2:  "request_ping",
	3:  "request_stats",
	4:  "request_run",
	5:  "request_check",
	6:  "request_free",
	7:  "request_terminate",
	8:  "request_soft_run",
	9:  "request_soft_check",
	10: "request_soft_free",
}

var RequestMsgid_gpbrpc_value = map[string]uint32{
	"request_rrd":        1,
	"request_ping":       2,
	"request_stats":      3,
	"request_run":        4,
	"request_check":      5,
	"request_free":       6,
	"request_terminate":  7,
	"request_soft_run":   8,
	"request_soft_check": 9,
	"request_soft_free":  10,
}

var ResponseMsgid_gpbrpc_name = map[uint32]string{
	1: "response_generic",
	2: "response_ping",
	3: "response_rrd",
	4: "response_stats",
	5: "response_check",
	6: "response_soft_check",
	7: "response_soft_run",
}

var ResponseMsgid_gpbrpc_value = map[string]uint32{
	"response_generic":    1,
	"response_ping":       2,
	"response_rrd":        3,
	"response_stats":      4,
	"response_check":      5,
	"response_soft_check": 6,
	"response_soft_run":   7,
}

func (GpbrpcType) GetRequestMsgid(msg proto.Message) uint32 {
	switch msg.(type) {
	case *RequestRrd:
		return uint32(RequestMsgid_REQUEST_RRD)
	case *RequestPing:
		return uint32(RequestMsgid_REQUEST_PING)
	case *RequestStats:
		return uint32(RequestMsgid_REQUEST_STATS)
	case *RequestRun:
		return uint32(RequestMsgid_REQUEST_RUN)
	case *RequestCheck:
		return uint32(RequestMsgid_REQUEST_CHECK)
	case *RequestFree:
		return uint32(RequestMsgid_REQUEST_FREE)
	case *RequestTerminate:
		return uint32(RequestMsgid_REQUEST_TERMINATE)
	case *RequestSoftRun:
		return uint32(RequestMsgid_REQUEST_SOFT_RUN)
	case *RequestSoftCheck:
		return uint32(RequestMsgid_REQUEST_SOFT_CHECK)
	case *RequestSoftFree:
		return uint32(RequestMsgid_REQUEST_SOFT_FREE)
	default:
		panic("you gave me the wrong message")
	}
}

func (GpbrpcType) GetRequestNameToIdMap() map[string]uint32 {
	return RequestMsgid_gpbrpc_value
}

func (GpbrpcType) GetRequestIdToNameMap() map[uint32]string {
	return RequestMsgid_gpbrpc_name
}

func (GpbrpcType) GetResponseNameToIdMap() map[string]uint32 {
	return ResponseMsgid_gpbrpc_value
}

func (GpbrpcType) GetResponseIdToNameMap() map[uint32]string {
	return ResponseMsgid_gpbrpc_name
}

func (GpbrpcType) GetResponseMsgid(msg proto.Message) uint32 {
	switch msg.(type) {
	case *ResponseGeneric:
		return uint32(ResponseMsgid_RESPONSE_GENERIC)
	case *ResponsePing:
		return uint32(ResponseMsgid_RESPONSE_PING)
	case *ResponseRrd:
		return uint32(ResponseMsgid_RESPONSE_RRD)
	case *ResponseStats:
		return uint32(ResponseMsgid_RESPONSE_STATS)
	case *ResponseCheck:
		return uint32(ResponseMsgid_RESPONSE_CHECK)
	case *ResponseSoftCheck:
		return uint32(ResponseMsgid_RESPONSE_SOFT_CHECK)
	case *ResponseSoftRun:
		return uint32(ResponseMsgid_RESPONSE_SOFT_RUN)
	default:
		panic("you gave me the wrong message")
	}
}

func (GpbrpcType) GetPackageName() string {
	return "badoo.phproxyd"
}

func (GpbrpcType) GetRequestMsg(request_msgid uint32) proto.Message {
	switch RequestMsgid(request_msgid) {
	case RequestMsgid_REQUEST_RRD:
		return &RequestRrd{}
	case RequestMsgid_REQUEST_PING:
		return &RequestPing{}
	case RequestMsgid_REQUEST_STATS:
		return &RequestStats{}
	case RequestMsgid_REQUEST_RUN:
		return &RequestRun{}
	case RequestMsgid_REQUEST_CHECK:
		return &RequestCheck{}
	case RequestMsgid_REQUEST_FREE:
		return &RequestFree{}
	case RequestMsgid_REQUEST_TERMINATE:
		return &RequestTerminate{}
	case RequestMsgid_REQUEST_SOFT_RUN:
		return &RequestSoftRun{}
	case RequestMsgid_REQUEST_SOFT_CHECK:
		return &RequestSoftCheck{}
	case RequestMsgid_REQUEST_SOFT_FREE:
		return &RequestSoftFree{}
	default:
		return nil
	}
}

func (GpbrpcType) GetResponseMsg(response_msgid uint32) proto.Message {
	switch ResponseMsgid(response_msgid) {
	case ResponseMsgid_RESPONSE_GENERIC:
		return &ResponseGeneric{}
	case ResponseMsgid_RESPONSE_PING:
		return &ResponsePing{}
	case ResponseMsgid_RESPONSE_RRD:
		return &ResponseRrd{}
	case ResponseMsgid_RESPONSE_STATS:
		return &ResponseStats{}
	case ResponseMsgid_RESPONSE_CHECK:
		return &ResponseCheck{}
	case ResponseMsgid_RESPONSE_SOFT_CHECK:
		return &ResponseSoftCheck{}
	case ResponseMsgid_RESPONSE_SOFT_RUN:
		return &ResponseSoftRun{}
	default:
		return nil
	}
}

type GpbrpcInterface interface {
	RequestRrd(rctx gpbrpc.RequestT, request *RequestRrd) gpbrpc.ResultT
	RequestPing(rctx gpbrpc.RequestT, request *RequestPing) gpbrpc.ResultT
	RequestStats(rctx gpbrpc.RequestT, request *RequestStats) gpbrpc.ResultT
	RequestRun(rctx gpbrpc.RequestT, request *RequestRun) gpbrpc.ResultT
	RequestCheck(rctx gpbrpc.RequestT, request *RequestCheck) gpbrpc.ResultT
	RequestFree(rctx gpbrpc.RequestT, request *RequestFree) gpbrpc.ResultT
	RequestTerminate(rctx gpbrpc.RequestT, request *RequestTerminate) gpbrpc.ResultT
	RequestSoftRun(rctx gpbrpc.RequestT, request *RequestSoftRun) gpbrpc.ResultT
	RequestSoftCheck(rctx gpbrpc.RequestT, request *RequestSoftCheck) gpbrpc.ResultT
	RequestSoftFree(rctx gpbrpc.RequestT, request *RequestSoftFree) gpbrpc.ResultT
}

func (GpbrpcType) Dispatch(rctx gpbrpc.RequestT, abstract_service interface{}) gpbrpc.ResultT {

	service := abstract_service.(GpbrpcInterface)

	switch RequestMsgid(rctx.MessageId) {
	case RequestMsgid_REQUEST_RRD:
		r := rctx.Message.(*RequestRrd)
		return service.RequestRrd(rctx, r)
	case RequestMsgid_REQUEST_PING:
		r := rctx.Message.(*RequestPing)
		return service.RequestPing(rctx, r)
	case RequestMsgid_REQUEST_STATS:
		r := rctx.Message.(*RequestStats)
		return service.RequestStats(rctx, r)
	case RequestMsgid_REQUEST_RUN:
		r := rctx.Message.(*RequestRun)
		return service.RequestRun(rctx, r)
	case RequestMsgid_REQUEST_CHECK:
		r := rctx.Message.(*RequestCheck)
		return service.RequestCheck(rctx, r)
	case RequestMsgid_REQUEST_FREE:
		r := rctx.Message.(*RequestFree)
		return service.RequestFree(rctx, r)
	case RequestMsgid_REQUEST_TERMINATE:
		r := rctx.Message.(*RequestTerminate)
		return service.RequestTerminate(rctx, r)
	case RequestMsgid_REQUEST_SOFT_RUN:
		r := rctx.Message.(*RequestSoftRun)
		return service.RequestSoftRun(rctx, r)
	case RequestMsgid_REQUEST_SOFT_CHECK:
		r := rctx.Message.(*RequestSoftCheck)
		return service.RequestSoftCheck(rctx, r)
	case RequestMsgid_REQUEST_SOFT_FREE:
		r := rctx.Message.(*RequestSoftFree)
		return service.RequestSoftFree(rctx, r)
	default:
		panic("screw you")
	}
}

var okResult = gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(0)})

func (GpbrpcType) OK(args ...interface{}) gpbrpc.ResultT {
	if len(args) == 0 {
		return okResult
	}
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(0),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorGeneric(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_GENERIC)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorRunFailed(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_RUN_FAILED)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorAlreadyRunning(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_ALREADY_RUNNING)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorNotFound(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_NOT_FOUND)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorWorking(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_WORKING)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorNoMemory(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_NO_MEMORY)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorSuccessFinished(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_SUCCESS_FINISHED)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorFailedFinished(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_FAILED_FINISHED)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorWaitForFree(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_WAIT_FOR_FREE)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

func (GpbrpcType) ErrorStartChildFailed(args ...interface{}) gpbrpc.ResultT {
	return gpbrpc.Result(&ResponseGeneric{ErrorCode: proto.Int32(-int32(Errno_ERRNO_START_CHILD_FAILED)),
		ErrorText: proto.String(fmt.Sprint(args...))})
}

/*
	func ($receiver$) RequestRrd(rctx gpbrpc.RequestT, request *$proto$.RequestRrd) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestPing(rctx gpbrpc.RequestT, request *$proto$.RequestPing) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestStats(rctx gpbrpc.RequestT, request *$proto$.RequestStats) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestRun(rctx gpbrpc.RequestT, request *$proto$.RequestRun) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestCheck(rctx gpbrpc.RequestT, request *$proto$.RequestCheck) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestFree(rctx gpbrpc.RequestT, request *$proto$.RequestFree) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestTerminate(rctx gpbrpc.RequestT, request *$proto$.RequestTerminate) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestSoftRun(rctx gpbrpc.RequestT, request *$proto$.RequestSoftRun) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestSoftCheck(rctx gpbrpc.RequestT, request *$proto$.RequestSoftCheck) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

	func ($receiver$) RequestSoftFree(rctx gpbrpc.RequestT, request *$proto$.RequestSoftFree) gpbrpc.ResultT {
		return $proto$.Gpbrpc.ErrorGeneric("not implemented")
	}

*/

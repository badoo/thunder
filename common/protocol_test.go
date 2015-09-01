package common

import (
	"bytes"
	"reflect"
	"strings"
	"testing"

	"github.com/golang/protobuf/proto"
)

func TestTransport(t *testing.T) {
	var b bytes.Buffer

	testLines := &LogLines{
		FileName: proto.String("test filename"),
		Inode:    proto.Uint64(100),
		Offset:   proto.Int64(3),
		Lines: []string{
			"Test line 1\n",
			"Test line 2\n",
			"Test line 3\n",
			strings.Repeat("Supertest", 10000),
		},
	}

	err := SendMessage(&b, testLines)
	if err != nil {
		t.Errorf("Unexpected error when sending message: %s", err.Error())
		return
	}

	decodedLines, err := ReceiveMessage(&b)
	if err != nil {
		t.Errorf("Unexpected error when receiving message: %s", err.Error())
		return
	}

	if !reflect.DeepEqual(testLines, decodedLines) {
		t.Errorf("Structures are not equal:\nExpected %+v,\nGot %+v", testLines, decodedLines)
		return
	}
}

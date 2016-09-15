package common

import (
	"errors"
	"io"

	"github.com/golang/protobuf/proto"
)

func SendMessage(c io.Writer, ln *LogLines) (err error) {
	var data []byte

	if data, err = proto.Marshal(ln); err != nil {
		return
	}

	l := uint32(len(data) + 4)

	_, err = c.Write([]byte{
		byte(l >> 24), // message length
		byte(l >> 16),
		byte(l >> 8),
		byte(l),
		0, // message id, currently always equal to zero
		0,
		0,
		0,
	})

	if err != nil {
		return
	}

	_, err = c.Write(data)
	return
}

func ReceiveMessage(c io.Reader) (ln *LogLines, err error) {
	var lenbuf = make([]byte, 4)
	if _, err = io.ReadFull(c, lenbuf); err != nil {
		return
	}

	l := uint32(uint32(lenbuf[0])<<24 + uint32(lenbuf[1])<<16 + uint32(lenbuf[2])<<8 + uint32(lenbuf[3]))
	if l <= 4 {
		err = errors.New("Message length must be greater than 4 bytes")
		return
	}

	buf := make([]byte, l)
	if _, err = io.ReadFull(c, buf); err != nil {
		return
	}

	// we ignore first 4 bytes as they are message id which is always equal to zero in current implementation
	ln = new(LogLines)
	err = proto.Unmarshal(buf[4:], ln)
	return
}

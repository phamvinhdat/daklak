// Copyright Pham Vinh Dat
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package record

import (
	"crypto/md5"
	"encoding/binary"
	"io"
)

const (
	HeaderSize = 1 + 4 + 4 + md5.Size
)

type Type int8

const (
	TypePersistence Type = iota
	TypeTTL
)

type Header struct {
	Type       Type
	KeyLength  uint32
	DataLength uint32
	Checksum   []byte
}

func (h Header) Marshal() []byte {
	headerBytes := make([]byte, HeaderSize)
	headerBytes[0] = byte(h.Type)
	binary.LittleEndian.PutUint32(headerBytes[1:], h.KeyLength)
	binary.LittleEndian.PutUint32(headerBytes[1+4:], h.DataLength)
	for i, b := range h.Checksum {
		headerBytes[1+4+4+i] = b
	}

	return headerBytes
}

func (h *Header) Unmarshal(b []byte) error {
	h.Type = Type(b[0])
	h.KeyLength = binary.LittleEndian.Uint32(b[1:])
	h.DataLength = binary.LittleEndian.Uint32(b[1+4:])
	h.Checksum = b[1+4+4 : HeaderSize]
	return nil
}

func (h *Header) FromReader(reader io.Reader) error {
	makeHeader := make([]byte, HeaderSize)
	_, err := reader.Read(makeHeader)
	if err != nil {
		return err
	}

	return h.Unmarshal(makeHeader)
}

func NewHeader(b []byte) (*Header, error) {
	h := &Header{}
	err := h.Unmarshal(b)
	return h, err
}

func (h *Header) BodySize() int64 {
	s := int64(h.KeyLength + h.DataLength)
	if h.Type == TypeTTL {
		s += 8
	}

	return s
}

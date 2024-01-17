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
	"time"

	"github.com/golang/snappy"
)

type Record struct {
	Header     *Header
	ExpiatedAt *time.Time
	Key        string
	Value      []byte
}

func NewRecord(key string, value []byte, ttl *time.Duration) *Record {
	r := &Record{
		Key:   key,
		Value: value,
	}

	if ttl != nil {
		t := time.Now().Add(*ttl)
		r.ExpiatedAt = &t
	}

	return r
}

func (r *Record) Marshal() []byte {
	var (
		encoded  []byte
		checksum = [md5.Size]byte{}
	)
	if len(r.Value) > 0 {
		encoded = snappy.Encode(nil, r.Value)
		checksum = md5.Sum(encoded)
	}

	h := &Header{
		KeyLength:  uint32(len(r.Key)),
		DataLength: uint32(len(encoded)),
		Checksum:   checksum[:],
	}

	if r.ExpiatedAt != nil {
		h.Type = TypeTTL
	}

	body := make([]byte, 0, h.BodySize())
	if h.Type == TypeTTL {
		ttlBytes := make([]byte, 8)
		unixMilli := r.ExpiatedAt.UnixMilli()
		binary.LittleEndian.PutUint64(ttlBytes, uint64(unixMilli))
		body = append(body, ttlBytes...)
	}

	body = append(body, []byte(r.Key)...)
	body = append(body, encoded...)
	headerBytes := h.Marshal()
	return append(headerBytes, body...)
}

func (r *Record) FromReader(reader io.Reader) error {
	bytesHeader := make([]byte, HeaderSize)
	_, err := reader.Read(bytesHeader)
	if err != nil {
		return err
	}

	h := &Header{}
	if err = h.Unmarshal(bytesHeader); err != nil {
		return err
	}

	kv := make([]byte, h.BodySize())
	_, err = reader.Read(kv)
	if err != nil {
		return err
	}

	var off uint32
	if h.Type == TypeTTL {
		num := binary.LittleEndian.Uint64(kv)
		t := time.UnixMilli(int64(num))
		r.ExpiatedAt = &t
		off = 8
	}

	r.Key = string(kv[off : off+h.KeyLength])
	r.Header = h
	r.Value, err = snappy.Decode(nil, kv[off+h.KeyLength:])
	return err
}

func (r Record) Size() int64 {
	return r.Header.BodySize() + HeaderSize
}

func (r Record) Valid() bool {
	if r.Header.DataLength == 0 {
		return false
	}

	if r.ExpiatedAt != nil && r.ExpiatedAt.Before(time.Now()) {
		return false
	}

	return true
}

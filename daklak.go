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

package daklak

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/golang/snappy"

	"github.com/phamvinhdat/daklak/record"
)

var (
	mKeys      *sync.Map
	lastOffset int64
)

func MapKeys() *sync.Map {
	return mKeys
}

type Daklak struct {
	mu     sync.RWMutex
	reader *os.File
	writer io.WriteCloser
}

func NewDaklak(path string) (*Daklak, error) {
	if err := os.MkdirAll(path, os.ModePerm); err != nil {
		return nil, err
	}

	filePath := filepath.Join(path, dataFile)
	writer, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}

	mKeys, err = load(reader)
	if err != nil {
		return nil, err
	}

	return &Daklak{
		reader: reader,
		writer: writer,
	}, nil
}

func (d *Daklak) Get(key string) ([]byte, error) {
	offset, ok := mKeys.Load(key)
	if !ok {
		return nil, ErrResourceNotFound
	}

	off := offset.(int64)
	headerBytes := make([]byte, record.HeaderSize)
	_, err := d.reader.ReadAt(headerBytes, off)
	if err != nil {
		return nil, err
	}

	h, err := record.NewHeader(headerBytes)
	if err != nil {
		return nil, err
	}

	r := &record.Record{
		Header: h,
	}

	if h.Type == record.TypeTTL {
		b := make([]byte, 8)
		_, err = d.reader.ReadAt(b, off+record.HeaderSize)
		num := binary.LittleEndian.Uint64(b)
		t := time.UnixMilli(int64(num))
		r.ExpiatedAt = &t
		off += 8
	}

	if !r.Valid() {
		mKeys.Delete(key)
		return nil, ErrResourceNotFound
	}

	value := make([]byte, r.Header.DataLength)
	_, err = d.reader.ReadAt(value, off+record.HeaderSize+int64(len(key)))
	if err != nil {
		return nil, err
	}

	value, err = snappy.Decode(nil, value)
	if err != nil {
		return nil, err
	}

	return value, nil
}

func (d *Daklak) Set(key string, value []byte) error {
	r := record.NewRecord(key, value, nil)
	b := r.Marshal()

	d.mu.Lock()
	defer d.mu.Unlock()
	n, err := d.writer.Write(b)
	if err != nil {
		return err
	}

	mKeys.Store(key, lastOffset)
	lastOffset += int64(n)
	return nil
}

func (d *Daklak) SetEx(key string, value []byte, ttl time.Duration) error {
	r := record.NewRecord(key, value, &ttl)
	b := r.Marshal()

	d.mu.Lock()
	defer d.mu.Unlock()
	n, err := d.writer.Write(b)
	if err != nil {
		return err
	}

	mKeys.Store(key, lastOffset)
	lastOffset += int64(n)
	return nil
}

func (d *Daklak) Delete(key string) error {
	if _, ok := mKeys.Load(key); !ok {
		return ErrResourceNotFound
	}

	r := record.NewRecord(key, []byte{}, nil)
	b := r.Marshal()
	d.mu.Lock()
	defer d.mu.Unlock()
	n, err := d.writer.Write(b)
	if err != nil {
		return err
	}

	mKeys.Delete(key)
	lastOffset += int64(n)
	return nil
}

func (d *Daklak) Close() error {
	var returnErr error
	if err := d.writer.Close(); err != nil {
		returnErr = err
	}

	if err := d.reader.Close(); err != nil {
		returnErr = err
	}

	return returnErr
}

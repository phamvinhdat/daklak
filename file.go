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
	"io"
	"os"
	"sync"

	"github.com/phamvinhdat/daklak/record"
)

func load(f *os.File) (*sync.Map, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	mKeys := new(sync.Map)
	fileSize := info.Size()
	if fileSize == 0 {
		return mKeys, nil
	}

	for {
		r := &record.Record{}
		if err := r.FromReader(f); err != nil {
			if err != io.EOF {
				return nil, err
			}

			break
		}

		if !r.Valid() { // delete tombstone or expiated
			lastOffset += r.Size()
			mKeys.Delete(r.Key)
			continue
		}

		mKeys.Store(r.Key, lastOffset)
		lastOffset += r.Size()
	}

	return mKeys, nil
}

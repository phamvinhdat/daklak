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

package benchmark

import (
	"math/rand"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/phamvinhdat/daklak"
	"github.com/phamvinhdat/daklak/benchmark/utils"
)

var (
	db   *daklak.Daklak
	mu   = &sync.RWMutex{}
	keys []string
)

func openDB() func() {
	dirPath := "./benchmark"

	var err error
	db, err = daklak.NewDaklak(dirPath)
	if err != nil {
		panic(err)
	}

	return func() {
		_ = db.Close()
		//_ = os.RemoveAll(options.DirPath)
	}
}

func BenchmarkPutGet(b *testing.B) {
	closer := openDB()
	defer closer()

	b.Run("set", benchmarkPut)
	b.Run("get", bencharkGet)
}

func benchmarkPut(b *testing.B) {
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := utils.GetTestKey(i)
		err := db.Set(key, utils.RandomValue(1024))
		assert.Nil(b, err)

		mu.Lock()
		keys = append(keys, key)
		mu.Unlock()
	}
}

func bencharkGet(b *testing.B) {
	for i := 0; i < 10000; i++ {
		key := utils.GetTestKey(i)
		err := db.Set(key, utils.RandomValue(1024))
		assert.Nil(b, err)

		mu.Lock()
		keys = append(keys, key)
		mu.Unlock()
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_, err := db.Get(randKey())
		if err != nil && err != daklak.ErrResourceNotFound {
			b.Fatal(err)
		}
	}
}

func randKey() string {
	mu.RLock()
	defer mu.RUnlock()

	return keys[rand.Intn(len(keys))]
}

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

package main

import (
	"fmt"

	"github.com/phamvinhdat/daklak"
)

func main() {
	d, err := daklak.NewDaklak("./test")
	if err != nil {
		panic(err)
	}

	defer d.Close()

	//err = d.Set("datpv", []byte("hehe heh ahdsh ashh dahsda sdhas dhasdh asdahsd ahsd"))
	//if err != nil {
	//	panic(err)
	//}

	//err = d.Set("123", []byte("hi, I'm a database"))
	//if err != nil {
	//	panic(err)
	//}

	for key, off := range daklak.MapKeys() {
		v, err := d.Get(key)
		if err != nil {
			panic(err)
		}

		fmt.Printf("key: %s, value: %s, off: %d\n", key, v, off)
	}
}

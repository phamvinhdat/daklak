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
	"errors"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/tidwall/redcon"

	"github.com/phamvinhdat/daklak"
)

func handler(db *daklak.Daklak) func(redcon.Conn, redcon.Command) {
	return func(conn redcon.Conn, cmd redcon.Command) {
		cmdStr := strings.ToLower(string(cmd.Args[0]))
		log.Printf("receiving command: %s\n", cmdStr)

		switch cmdStr {
		default:
			conn.WriteError("ERR unknown command '" + string(cmd.Args[0]) + "'")
		case "ping":
			conn.WriteString("PONG")
		case "quit":
			conn.WriteString("OK")
			conn.Close()
		case "select":
			conn.WriteString("OK")
		case "set":
			if len(cmd.Args) != 3 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			key := string(cmd.Args[1])
			val := cmd.Args[2]
			if err := db.Set(key, val); err != nil {
				conn.WriteError(err.Error())
				return
			}

			conn.WriteString("OK")
		case "setex":
			if len(cmd.Args) != 4 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			key := string(cmd.Args[1])
			ttlInSecond, err := strconv.Atoi(string(cmd.Args[2]))
			if err != nil {
				conn.WriteError("ERR error parsing ttl: " + err.Error())
				return
			}

			ttl := time.Duration(ttlInSecond) * time.Second
			val := cmd.Args[3]
			if err := db.SetEx(key, val, ttl); err != nil {
				conn.WriteError(err.Error())
				return
			}

			conn.WriteString("OK")
		case "get":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}
			key := string(cmd.Args[1])
			val, err := db.Get(key)
			if err != nil {
				if !errors.Is(err, daklak.ErrResourceNotFound) {
					conn.WriteError(err.Error())
					return
				}

				conn.WriteNull()
				return
			}

			conn.WriteBulk(val)
		case "keys":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			pattern := string(cmd.Args[1])
			// Fast-track condition for improved speed
			if pattern == "*" {
				n := 0
				var raw []byte
				daklak.MapKeys().Range(func(key, _ any) bool {
					n++
					raw = redcon.AppendBulk(raw, []byte(key.(string)))
					return true
				})

				prefix := redcon.AppendArray([]byte{}, n)
				conn.WriteRaw(append(prefix, raw...))
				return
			}

			conn.WriteArray(0)
		case "del":
			if len(cmd.Args) != 2 {
				conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
				return
			}

			key := string(cmd.Args[1])
			if err := db.Delete(key); err != nil {
				if !errors.Is(err, daklak.ErrResourceNotFound) {
					conn.WriteError(err.Error())
					return
				}

				conn.WriteInt(0)
				return
			}

			conn.WriteInt(1)
			//case "publish":
			//	if len(cmd.Args) != 3 {
			//		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			//		return
			//	}
			//	conn.WriteInt(ps.Publish(string(cmd.Args[1]), string(cmd.Args[2])))
			//case "subscribe", "psubscribe":
			//	if len(cmd.Args) < 2 {
			//		conn.WriteError("ERR wrong number of arguments for '" + string(cmd.Args[0]) + "' command")
			//		return
			//	}
			//	command := strings.ToLower(string(cmd.Args[0]))
			//	for i := 1; i < len(cmd.Args); i++ {
			//		if command == "psubscribe" {
			//			ps.Psubscribe(conn, string(cmd.Args[i]))
			//		} else {
			//			ps.Subscribe(conn, string(cmd.Args[i]))
			//		}
			//	}
		}
	}
}

func isAccepted(conn redcon.Conn) bool {
	// Use this function to accept or deny the connection.
	log.Printf("accept: %s", conn.RemoteAddr())
	return true
}

func isClosed(conn redcon.Conn, err error) {
	// This is called when the connection has been closed
	log.Printf("closed: %s, err: %v", conn.RemoteAddr(), err)
}

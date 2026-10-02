package main

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

type Command struct {
	value        any
	length       int //this attribute is 0 in command types with no length value
	command_type string
}

func handleConnection(conn net.Conn, storage *Storage) {
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		resp := make([]byte, 1024)
		cmd := parseRESP(string(buf[:n]))

		if cmd.command_type != ARRAY {
			resp = encode_response(SIM_ERR, "ERR invalid request type")
		} else {
			t_cmd := cmd.value.([]*Command)
			for i := 0; i < cmd.length; i++ {
				switch t_cmd[i].command_type {
				case BULK_STRINGS:
					switch strings.ToUpper(t_cmd[i].value.(string)) {
					case "PING":
						resp = encode_response(SIM_STRINGS, "PONG")
					case "ECHO":
						msg := t_cmd[i+1].value
						resp = encode_response(BULK_STRINGS, msg.(string))
						//Ignore the next iteration need to optimize later if possible
						i += 1
					case "SET":
						key := t_cmd[i+1].value.(string)
						value := t_cmd[i+2].value.(string)
						if cmd.length > 3 {
							arg := t_cmd[i+3].value.(string)
							arg_value, err := strconv.Atoi(t_cmd[i+4].value.(string))
							if err != nil {
								resp = encode_response(SIM_ERR, "Invalid time for expiration.")
							}
							storage.setWithExpiration(key, value, arg, arg_value)
							resp = encode_response(SIM_STRINGS, "OK")
							i += 4
							break
						}
						storage.set(key, value)
						resp = encode_response(SIM_STRINGS, "OK")
						//Ignore the next iteration need to optimize later if possible
						i += 2
					case "GET":
						key := t_cmd[i+1].value.(string)
						value, err := storage.get(key)
						if err != nil {
							resp = encode_response(NULL_BULK_STRING)
							break
						}
						resp = encode_response(BULK_STRINGS, value)
						//Ignore the next iteration need to optimize later if possible
						i += 1
					case "RPUSH":
						resp = encode_response(INTEGER, "1")
					default:
						resp = encode_response(SIM_ERR, "ERR invalid command type or support for command doesn't exist yet.")
					}
				}
			}

		}

		_, err = conn.Write(resp)
		if err != nil {
			conn.Close()
			return
		}

	}
}

func main() {
	storage := &Storage{kv: make(map[string]*Value)}
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379.")
		os.Exit(1)
	}
	fmt.Println("Started redis server in port 6379.")
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Failed to bind to port 6379.")
			os.Exit(1)
		}
		go handleConnection(conn, storage)
	}
}

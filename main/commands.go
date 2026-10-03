package main

import (
	"fmt"
	"strconv"
	"strings"
)

type Command struct {
	value        any
	length       int //this attribute is 0 in command types with no length value
	command_type string
}

func handleCommand(storage *Storage, cmd *Command) []byte {
	resp := make([]byte, 1024)
	if cmd.command_type != ARRAY {
		resp = encode_response(SIM_ERR, "ERR invalid request type")
	} else {
		t_cmd := cmd.value.([]*Command)
		i := 0
		switch t_cmd[i].command_type {
		case BULK_STRINGS:
			switch strings.ToUpper(t_cmd[i].value.(string)) {
			case "PING":
				resp = encode_response(SIM_STRINGS, "PONG")
			case "ECHO":
				args := args_parser(t_cmd)
				if len(args) != 1 {
					resp = encode_response(SIM_ERR, fmt.Sprintf("Invalid Arguments. Expected one got %d", len(args)))
					break
				}
				msg := t_cmd[i+1].value
				resp = encode_response(BULK_STRINGS, msg.(string))
			case "SET":
				args := args_parser(t_cmd)
				if len(args) < 2 || len(args) > 4 {
					resp = encode_response(SIM_ERR, "Invalid Arguments.")
					break
				}
				key := args[0]
				value := args[1]
				if len(args) > 2 {
					arg := args[2]
					arg_value, err := strconv.Atoi(args[3])
					if err != nil {
						resp = encode_response(SIM_ERR, "Invalid time for expiration.")
					}
					storage.setWithExpiration(key, value, arg, arg_value)
					resp = encode_response(SIM_STRINGS, "OK")
					break
				}
				storage.set(key, value)
				resp = encode_response(SIM_STRINGS, "OK")
			case "GET":
				args := args_parser(t_cmd)
				if len(args) != 1 {
					resp = encode_response(SIM_ERR, "Invalid Arguments.")
					break
				}
				key := args[0]
				value, err := storage.get(key)
				if err != nil {
					resp = encode_response(NULL_BULK_STRING)
					break
				}
				resp = encode_response(BULK_STRINGS, value)
			case "RPUSH":
				args := args_parser(t_cmd)
				if len(args) < 2 {
					resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
					break
				}
				key := args[0]
				length := storage.rpush(key, args[1:])
				resp = encode_response(INTEGER, strconv.Itoa(length))
			case "LPUSH":
				args := args_parser(t_cmd)
				if len(args) < 2 {
					resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
					break
				}
				key := args[0]
				length := storage.lpush(key, args[1:])
				resp = encode_response(INTEGER, strconv.Itoa(length))
			case "LPOP":
				args := args_parser(t_cmd)
				if len(args) != 1 {
					resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
					break
				}
				key := args[0]
				element, err := storage.lpop(key)
				if err != nil {
					resp = encode_response(NULL_BULK_STRING)
				}
				resp = encode_response(BULK_STRINGS, element)
			case "LRANGE":
				args := args_parser(t_cmd)
				if len(args) < 3 {
					resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
					break
				}
				key := args[0]
				start, err := strconv.Atoi(args[1])
				if err != nil {
					resp = encode_response(SIM_ERR, "Invalid index use a number.")
					break
				}
				stop, err := strconv.Atoi(args[2])
				if err != nil {
					resp = encode_response(SIM_ERR, "Invalid index use a number.")
					break
				}
				list := storage.lrange(key, start, stop)
				resp = encode_response(ARRAY, list...)
			case "LLEN":
				args := args_parser(t_cmd)
				if len(args) != 1 {
					resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
					break
				}
				key := args[0]
				length := storage.llen(key)
				resp = encode_response(INTEGER, strconv.Itoa(length))
			default:
				resp = encode_response(SIM_ERR, "ERR invalid command type or support for command doesn't exist yet.")
			}
		}

	}
	return resp
}

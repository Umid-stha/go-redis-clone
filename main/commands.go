package main

import (
	"context"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"
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
		return resp
	}
	t_cmd := cmd.value.([]*Command)
	switch t_cmd[0].command_type {
	case BULK_STRINGS:
		switch strings.ToUpper(t_cmd[0].value.(string)) {
		case "PING":
			resp = encode_response(SIM_STRINGS, "PONG")
		case "ECHO":
			args := args_parser(t_cmd)
			if len(args) != 1 {
				resp = encode_response(SIM_ERR, fmt.Sprintf("Invalid Arguments. Expected one got %d", len(args)))
				break
			}
			msg := args[0]
			resp = encode_response(BULK_STRINGS, msg)
		case "TYPE":
			args := args_parser(t_cmd)
			if len(args) != 1 {
				resp = encode_response(SIM_ERR, fmt.Sprintf("Invalid Arguments. Expected one got %d", len(args)))
				break
			}
			key := args[0]
			_, exists := storage.kv[key]
			if !exists {
				resp = encode_response(SIM_STRINGS, "none")
				break
			}
			resp = encode_response(SIM_STRINGS, "string")
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
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
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
			storage.mu.Lock()
			startIndex := len(storage.kv[key].Value.([]string))
			storage.mu.Unlock()
			length, err := storage.rpush(key, args[1:])
			if err != nil {
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
			}
			resp = encode_response(INTEGER, strconv.Itoa(length))
			// Implement checking of the blockqueue and if there is clients waiting pop the list and updated the block channel
			storage.mu.Lock()
			queue, exists := storage.blockQueue[key]
			storage.mu.Unlock()
			if !exists || len(queue) == 0 {
				break
			}
			element, _ := storage.ipop(key, startIndex)
			storage.mu.Lock()
			storage.blockQueue[key][0] <- element
			copy(storage.blockQueue[key], storage.blockQueue[key][1:])
			//Drop the last empty element
			storage.blockQueue[key] = storage.blockQueue[key][:len(storage.blockQueue[key])-1]
			storage.mu.Unlock()
		case "LPUSH":
			args := args_parser(t_cmd)
			if len(args) < 2 {
				resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
				break
			}
			key := args[0]
			length, err := storage.lpush(key, args[1:])
			if err != nil {
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
			}
			resp = encode_response(INTEGER, strconv.Itoa(length))
			// Implement checking of the blockqueue and if there is clients waiting pop the list and updated the block channel
			storage.mu.Lock()
			queue, exists := storage.blockQueue[key]
			storage.mu.Unlock()
			if !exists || len(queue) == 0 {
				break
			}
			element, _ := storage.ipop(key, 0)
			storage.mu.Lock()
			storage.blockQueue[key][0] <- element
			copy(storage.blockQueue[key], storage.blockQueue[key][1:])
			//Drop the last empty element
			storage.blockQueue[key] = storage.blockQueue[key][:len(storage.blockQueue[key])-1]
			storage.mu.Unlock()
		case "LPOP":
			args := args_parser(t_cmd)
			if len(args) < 1 || len(args) > 2 {
				resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
				break
			}
			key := args[0]
			if len(args) == 2 {
				num, err := strconv.Atoi(args[1])
				if err != nil {
					resp = encode_response(SIM_ERR, "Invalid index use a number.")
					break
				}
				elements, err := storage.mLpop(key, num)
				if err != nil {
					resp = encode_response(NULL_BULK_STRING)
					break
				}
				resp = encode_response(ARRAY, elements...)
				break
			}
			element, err := storage.lpop(key)
			if err != nil {
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
				resp = encode_response(NULL_BULK_STRING)
				break
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
			list, err := storage.lrange(key, start, stop)
			if err != nil {
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
			}
			resp = encode_response(ARRAY, list...)
		case "LLEN":
			args := args_parser(t_cmd)
			if len(args) != 1 {
				resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
				break
			}
			key := args[0]
			length, err := storage.llen(key)
			if err != nil {
				if err == ErrWrongType {
					resp = encode_response(SIM_ERR, err.Error())
					break
				}
			}
			resp = encode_response(INTEGER, strconv.Itoa(length))
		case "BLPOP":
			args := args_parser(t_cmd)
			if len(args) != 2 {
				resp = encode_response(SIM_ERR, "Invalid Number of arguments.")
				break
			}
			key := args[0]
			timeout, err := strconv.Atoi(args[1])
			if err != nil {
				resp = encode_response(SIM_ERR, "Invalid timeout use a number.")
				break
			}
			ch := make(chan string, 1)
			storage.mu.Lock()
			storage.blockQueue[key] = append(storage.blockQueue[key], ch)
			storage.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeout)*time.Second)
			if timeout == 0 {
				ctx = context.Background()
			}
			defer cancel()
			select {
			case element := <-ch:
				resp = encode_response(ARRAY, key, element)
				close(ch)
			case <-ctx.Done():
				storage.mu.Lock()
				list := storage.blockQueue[key]
				index := slices.Index(list, ch)
				copy(list[:index], list[index+1:])
				//Drop the last empty element
				storage.blockQueue[key] = list[:len(list)-1]
				storage.mu.Unlock()
				resp = encode_response(NULL_ARRAY)
			}
		default:
			resp = encode_response(SIM_ERR, "ERR invalid command type or support for command doesn't exist yet.")
		}
	}
	return resp
}

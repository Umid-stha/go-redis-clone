package main

import (
	"strconv"
	"strings"
)

func getCommand(msg_arr []string, counter *int) *Command {
	cmd := &Command{}
	value := msg_arr[*counter]
	for i, v := range value {
		switch string(v) {
		case BULK_STRINGS:
			cmd.length, _ = strconv.Atoi(string(value[i+1]))
			cmd.command_type = BULK_STRINGS
			*counter += 1
			cmd.value = msg_arr[*counter]
			return cmd
		case ARRAY:
			cmd.length, _ = strconv.Atoi(string(value[i+1]))
			cmd.command_type = ARRAY
			*counter += 1
			commands := make([]*Command, cmd.length)
			for j := 0; j < cmd.length; j++ {
				commands[j] = getCommand(msg_arr, counter)
				*counter += 1
			}
			cmd.value = commands
			return cmd
		}
	}
	return cmd
}

/*
Q: What am i expecting from parseRESP

Thinking:
**None of the code is actual code and only pseudo code/easy to understand code**
  - i need the exact command that i get from redis-cli
  - for example : PING i need command.value to be PING which will be handled from conn.read
  - but the command can also be a array and how i will determine that will depend on the command.type
  - so if command is a array then command.value will be of type []*command | for example:
  - redis-cli ECHO hey -> command.value = [Command(value: ECHO, type: bulk_string), Command(value: hey, type: bulk_string)]
*/
func parseRESP(message string) *Command {
	message = strings.TrimSuffix(message, "\r\n")
	msg_arr := strings.Split(message, "\r\n")
	counter := 0
	cmd := getCommand(msg_arr, &counter)
	return cmd
}

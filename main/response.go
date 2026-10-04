package main

import (
	"fmt"
	"strings"
)

func encode_response(resp_type string, args ...string) []byte {
	var b strings.Builder
	switch resp_type {
	case ARRAY:
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
		}
	case BULK_STRINGS:
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(args[0]), args[0])
	case SIM_STRINGS:
		fmt.Fprintf(&b, "+%s\r\n", args[0])
	case SIM_ERR:
		fmt.Fprintf(&b, "-%s\r\n", args[0])
	case NULL_BULK_STRING:
		fmt.Fprintf(&b, "$-1\r\n")
	case NULL_ARRAY:
		fmt.Fprintf(&b, "*-1\r\n")
	case INTEGER:
		fmt.Fprintf(&b, ":%s\r\n", args[0])
	default:
		return []byte("")
	}
	return []byte(b.String())
}

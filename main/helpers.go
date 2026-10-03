package main

/*
* Returns array of arguments after ignoring the first command bulk string
 */
func args_parser(cmd_arr []*Command) []string {
	length := len(cmd_arr)
	//ignoring the command bulk string, hence the -1.
	length_of_args := length - 1
	elements := make([]string, length_of_args)
	for j := 0; j < length_of_args; j++ {
		elements[j] = cmd_arr[j+1].value.(string)
	}
	return elements
}

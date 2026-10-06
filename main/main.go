package main

import (
	"fmt"
	"net"
	"os"
)

func handleConnection(conn net.Conn, storage *Storage) {
	buf := make([]byte, 1024)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			conn.Close()
			return
		}
		cmd := parseRESP(string(buf[:n]))
		resp := handleCommand(storage, cmd)

		_, err = conn.Write(resp)
		if err != nil {
			conn.Close()
			return
		}

	}
}

func main() {
	storage := &Storage{
		kv:         make(map[string]*Value),
		blockQueue: make(map[string][]chan string),
	}
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
			continue
		}
		fmt.Printf("Accepted conn from %s\n", conn.RemoteAddr().String())
		go handleConnection(conn, storage)
	}
}

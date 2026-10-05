// Package redistest is a standalone RESP test harness for a Redis-in-Go
// server. It has zero dependency on your implementation — it only speaks the
// wire protocol, connects to your already-running server, and checks replies.
// That also means it works for any language, not just Go, if you end up
// reusing it elsewhere.
//
// Usage:
//  1. Start your server (e.g. `go run .` in your redis project, listening on :6379)
//  2. Drop this whole `redistest/` folder inside your module (or run it as its
//     own module — it has no external deps, just the standard library)
//  3. Run everything:          go test ./redistest -v
//  4. Run one stage's test:    go test ./redistest -run TestExpiry -v
//  5. Different port/host:     REDIS_ADDR=localhost:6380 go test ./redistest -v
//
// Stage → test mapping (matches the Go Redis learning track):
//
//	ping                                 -> TestPing
//	handle multiple commands             -> covered implicitly (every test reuses one conn for several commands)
//	concurrent clients                   -> TestConcurrentClients
//	echo                                 -> TestEcho
//	set/get                              -> TestSetGet, TestGetMissing
//	expiry (PX)                          -> TestExpiry
//	del/exists                           -> TestDelExists
//	type                                 -> TestType
//	lpush/rpush/llen/lrange/lpop/rpop    -> TestLists
//	blocking blpop                       -> TestBlockingLPop
//	multi/exec/discard                   -> TestMultiExec, TestDiscard
//	errors inside a transaction          -> TestQueueError
//	xadd/xrange/xread                    -> TestStreams
//	blocking xread                       -> TestBlockingXRead
//	subscribe/publish                    -> TestPubSub
//	RDB stages (flags/header/keys/expiry) -> not here, see ../MANUAL-TESTING.md
//	replication stages                   -> not here, see ../MANUAL-TESTING.md
package redistest

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func addr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

// client wraps one connection with just enough RESP encode/decode to test with.
type client struct {
	conn net.Conn
	r    *bufio.Reader
}

func dial(t *testing.T) *client {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("could not connect to %s — is your server running? (%v)", addr(), err)
	}
	return &client{conn: conn, r: bufio.NewReader(conn)}
}

func (c *client) close() { c.conn.Close() }

// send encodes args as a RESP array of bulk strings: send("SET","k","v").
func (c *client) send(args ...string) {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(args))
	for _, a := range args {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
	}
	c.conn.Write([]byte(b.String()))
}

// read parses exactly one RESP reply:
//
//	simple string / bulk string -> string
//	integer                     -> int64
//	error                       -> non-nil error
//	array                       -> []interface{}
//	nil bulk/array              -> nil
func (c *client) read() (interface{}, error) {
	line, err := c.r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if len(line) == 0 {
		return nil, fmt.Errorf("empty reply line")
	}
	switch line[0] {
	case '+':
		return line[1:], nil
	case '-':
		return nil, fmt.Errorf("%s", line[1:])
	case ':':
		n, _ := strconv.ParseInt(line[1:], 10, 64)
		return n, nil
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n == -1 {
			return nil, nil
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, _ := strconv.Atoi(line[1:])
		if n == -1 {
			return nil, nil
		}
		arr := make([]interface{}, n)
		for i := 0; i < n; i++ {
			v, err := c.read()
			if err != nil {
				return nil, err
			}
			arr[i] = v
		}
		return arr, nil
	default:
		return nil, fmt.Errorf("unexpected reply prefix %q", line[0])
	}
}

// ---- Foundations ----

func TestPing(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("PING")
	got, err := c.read()
	if err != nil {
		t.Fatalf("PING errored: %v", err)
	}
	if got != "PONG" {
		t.Fatalf("PING = %v, want PONG", got)
	}
}

func TestConcurrentClients(t *testing.T) {
	c1 := dial(t)
	defer c1.close()
	c2 := dial(t)
	defer c2.close()
	c1.send("PING")
	c2.send("PING")
	r1, err1 := c1.read()
	r2, err2 := c2.read()
	if err1 != nil || err2 != nil {
		t.Fatalf("errors from two simultaneous clients: %v, %v", err1, err2)
	}
	if r1 != "PONG" || r2 != "PONG" {
		t.Fatalf("got %v, %v — want PONG, PONG from both", r1, r2)
	}
}

func TestEcho(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("ECHO", "hello")
	got, err := c.read()
	if err != nil {
		t.Fatalf("ECHO errored: %v", err)
	}
	if got != "hello" {
		t.Fatalf("ECHO = %v, want hello", got)
	}
}

// ---- Data commands ----

func TestSetGet(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("SET", "foo", "bar")
	if _, err := c.read(); err != nil {
		t.Fatalf("SET errored: %v", err)
	}
	c.send("GET", "foo")
	got, err := c.read()
	if err != nil {
		t.Fatalf("GET errored: %v", err)
	}
	if got != "bar" {
		t.Fatalf("GET foo = %v, want bar", got)
	}
}

func TestGetMissing(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("GET", "no-such-key-xyz123")
	got, err := c.read()
	if err != nil {
		t.Fatalf("GET errored: %v", err)
	}
	if got != nil {
		t.Fatalf("GET missing key = %v, want nil", got)
	}
}

func TestExpiry(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("SET", "temp", "gone-soon", "PX", "100")
	if _, err := c.read(); err != nil {
		t.Fatalf("SET errored: %v", err)
	}
	c.send("GET", "temp")
	if got, _ := c.read(); got != "gone-soon" {
		t.Fatalf("GET before expiry = %v, want gone-soon", got)
	}
	time.Sleep(150 * time.Millisecond)
	c.send("GET", "temp")
	if got, _ := c.read(); got != nil {
		t.Fatalf("GET after expiry = %v, want nil", got)
	}
}

func TestDelExists(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("SET", "delme", "x")
	c.read()
	c.send("EXISTS", "delme")
	if got, _ := c.read(); got != int64(1) {
		t.Fatalf("EXISTS before DEL = %v, want 1", got)
	}
	c.send("DEL", "delme")
	if got, _ := c.read(); got != int64(1) {
		t.Fatalf("DEL = %v, want 1", got)
	}
	c.send("EXISTS", "delme")
	if got, _ := c.read(); got != int64(0) {
		t.Fatalf("EXISTS after DEL = %v, want 0", got)
	}
}

func TestType(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("SET", "typed", "x")
	c.read()
	c.send("TYPE", "typed")
	if got, _ := c.read(); got != "string" {
		t.Fatalf("TYPE = %v, want string", got)
	}
	c.send("TYPE", "nope-not-here")
	if got, _ := c.read(); got != "none" {
		t.Fatalf("TYPE of missing key = %v, want none", got)
	}
}

// ---- Lists ----

func TestLists(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("RPUSH", "mylist", "a", "b", "c")
	if got, err := c.read(); err != nil || got != int64(3) {
		t.Fatalf("RPUSH length = %v (err %v), want 3", got, err)
	}
	c.send("LPUSH", "mylist", "z")
	if got, err := c.read(); err != nil || got != int64(4) {
		t.Fatalf("LPUSH length = %v (err %v), want 4", got, err)
	}
	c.send("LLEN", "mylist")
	if got, err := c.read(); err != nil || got != int64(4) {
		t.Fatalf("LLEN = %v (err %v), want 4", got, err)
	}
	c.send("LRANGE", "mylist", "0", "-1")
	got, err := c.read()
	if err != nil {
		t.Fatalf("LRANGE errored: %v", err)
	}
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 4 || arr[0] != "z" {
		t.Fatalf("LRANGE = %v, want [z a b c]", got)
	}
	c.send("LPOP", "mylist")
	if got, _ := c.read(); got != "z" {
		t.Fatalf("LPOP = %v, want z", got)
	}
	c.send("RPOP", "mylist")
	if got, _ := c.read(); got != "c" {
		t.Fatalf("RPOP = %v, want c", got)
	}
}

func TestBlockingLPop(t *testing.T) {
	writer := dial(t)
	defer writer.close()
	reader := dial(t)
	defer reader.close()

	done := make(chan interface{}, 1)
	go func() {
		reader.send("BLPOP", "blocklist", "1")
		v, _ := reader.read()
		done <- v
	}()
	time.Sleep(100 * time.Millisecond)
	writer.send("RPUSH", "blocklist", "val")
	if _, err := writer.read(); err != nil {
		t.Fatalf("RPUSH errored: %v", err)
	}

	select {
	case v := <-done:
		arr, ok := v.([]interface{})
		if !ok || len(arr) != 2 || arr[0] != "blocklist" || arr[1] != "val" {
			t.Fatalf("BLPOP returned %v, want [blocklist val]", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("BLPOP never returned after RPUSH — are you waking blocked waiters?")
	}
}

// ---- Transactions ----

func TestMultiExec(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("MULTI")
	if _, err := c.read(); err != nil {
		t.Fatalf("MULTI errored: %v", err)
	}
	c.send("SET", "tx", "1")
	if _, err := c.read(); err != nil {
		t.Fatalf("queued SET errored: %v", err)
	}
	c.send("GET", "tx")
	if _, err := c.read(); err != nil {
		t.Fatalf("queued GET errored: %v", err)
	}
	c.send("EXEC")
	got, err := c.read()
	if err != nil {
		t.Fatalf("EXEC errored: %v", err)
	}
	arr, ok := got.([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("EXEC reply = %v, want a 2-element array", got)
	}
	if arr[1] != "1" {
		t.Fatalf("EXEC result[1] = %v, want 1", arr[1])
	}
}

func TestDiscard(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("MULTI")
	c.read()
	c.send("SET", "discarded", "yes")
	c.read()
	c.send("DISCARD")
	if _, err := c.read(); err != nil {
		t.Fatalf("DISCARD errored: %v", err)
	}
	c.send("GET", "discarded")
	if got, _ := c.read(); got != nil {
		t.Fatalf("GET after DISCARD = %v, want nil — the queued SET should never have run", got)
	}
}

func TestQueueError(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("MULTI")
	c.read()
	c.send("NOTACOMMAND")
	if _, err := c.read(); err == nil {
		t.Fatalf("queueing an unknown command should return an error immediately")
	}
	c.send("SET", "after-error", "x")
	c.read()
	c.send("EXEC")
	got, execErr := c.read()
	if execErr == nil && got != nil {
		t.Fatalf("EXEC after a queuing-time error returned %v — real Redis aborts the whole transaction", got)
	}
}

// ---- Streams ----

func TestStreams(t *testing.T) {
	c := dial(t)
	defer c.close()
	c.send("XADD", "mystream", "*", "field", "value1")
	if _, err := c.read(); err != nil {
		t.Fatalf("XADD errored: %v", err)
	}
	c.send("XADD", "mystream", "*", "field", "value2")
	if _, err := c.read(); err != nil {
		t.Fatalf("second XADD errored: %v", err)
	}
	c.send("XRANGE", "mystream", "-", "+")
	got, err := c.read()
	if err != nil {
		t.Fatalf("XRANGE errored: %v", err)
	}
	if arr, ok := got.([]interface{}); !ok || len(arr) != 2 {
		t.Fatalf("XRANGE returned %v, want 2 entries", got)
	}
	c.send("XREAD", "STREAMS", "mystream", "0")
	got, err = c.read()
	if err != nil {
		t.Fatalf("XREAD errored: %v", err)
	}
	if got == nil {
		t.Fatalf("XREAD from 0 returned nil, want both entries")
	}
}

func TestBlockingXRead(t *testing.T) {
	writer := dial(t)
	defer writer.close()
	reader := dial(t)
	defer reader.close()

	done := make(chan interface{}, 1)
	go func() {
		reader.send("XREAD", "BLOCK", "1000", "STREAMS", "blocktest", "$")
		v, _ := reader.read()
		done <- v
	}()
	time.Sleep(100 * time.Millisecond) // give the blocking read time to register
	writer.send("XADD", "blocktest", "*", "k", "v")
	if _, err := writer.read(); err != nil {
		t.Fatalf("XADD errored: %v", err)
	}

	select {
	case v := <-done:
		if v == nil {
			t.Fatalf("blocking XREAD returned nil, want the new entry")
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("blocking XREAD never returned after XADD — are you waking blocked readers?")
	}
}

// ---- Pub/Sub ----

func TestPubSub(t *testing.T) {
	sub := dial(t)
	defer sub.close()
	pub := dial(t)
	defer pub.close()

	sub.send("SUBSCRIBE", "news")
	if _, err := sub.read(); err != nil {
		t.Fatalf("SUBSCRIBE errored: %v", err)
	}

	msg := make(chan interface{}, 1)
	go func() {
		v, _ := sub.read()
		msg <- v
	}()

	time.Sleep(100 * time.Millisecond)
	pub.send("PUBLISH", "news", "hello subscribers")
	if _, err := pub.read(); err != nil {
		t.Fatalf("PUBLISH errored: %v", err)
	}

	select {
	case v := <-msg:
		arr, ok := v.([]interface{})
		if !ok || len(arr) < 3 || arr[2] != "hello subscribers" {
			t.Fatalf("subscriber received %v, want a message array ending in the published text", v)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("subscriber never received the published message")
	}
}

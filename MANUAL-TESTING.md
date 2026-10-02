# Manual testing protocol

`redistest/redis_test.go` covers everything that's just one server + plain TCP.
Two families of stages need real process/file orchestration and are easier to
check by hand than to automate portably: **RDB persistence** and **replication**.

## RDB persistence

**Config flags**
```
./your_server --dir /tmp/redis-files --dbfilename dump.rdb
redis-cli CONFIG GET dir         # -> ["dir", "/tmp/redis-files"]
redis-cli CONFIG GET dbfilename  # -> ["dbfilename", "dump.rdb"]
```

**Header / keys / expiry** — you need a real RDB file to load. Easiest way to
get one: point a real `redis-server` at the same directory, write some keys,
save, then point *your* server at the same file.
```
redis-server --dir /tmp/redis-files --dbfilename dump.rdb &
redis-cli SET alpha 1
redis-cli SET beta 2 PX 60000
redis-cli SAVE
kill %1

# now start your server against that file and check:
redis-cli GET alpha   # -> 1
redis-cli GET beta    # -> 2 right away, nil after ~60s
```
No real Redis installed to generate a fixture? Say the word and I'll put
together a minimal RDB file (or a small script that generates one) so you can
test your parser without needing another Redis binary around.

## Replication

```
./your_server --port 6379                              # master
./your_server --port 6380 --replicaof "localhost 6379" # replica
```

- **Handshake:** watch stdout/logs on the master for the replica's PING, two
  REPLCONFs, then PSYNC — add temporary print statements if you don't have
  logging yet.
- **Propagation:**
  ```
  redis-cli -p 6379 SET k v
  redis-cli -p 6380 GET k   # -> v, without ever writing to the replica directly
  ```
- **WAIT:**
  ```
  redis-cli -p 6379 SET k2 v2
  redis-cli -p 6379 WAIT 1 1000   # -> 1 once the replica has acked
  ```

## Running the automated half

```
go test ./redistest -v                              # everything
go test ./redistest -run TestExpiry -v               # just one stage
REDIS_ADDR=localhost:6380 go test ./redistest -v      # against a replica, etc.
```

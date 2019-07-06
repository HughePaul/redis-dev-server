# redis-dev-server
Non-production minimal redis server supporting a small subset of redis commands

## Usage
```
npm install redis-dev-server
node . [ -a address ] [ -p port ] [ -i save_interval | -n ] [ -e expire_interval ] [ persist_filename ]
```
eg:
```
node . -p 1234 myfile.db --save-interval 5
```

## Supported commands
- `INFO` - responds with `+OK`
- `PING` - responds with `+PONG`
- `CLIENT`
- - `SETNAME name` - will use this name in logging
- - `GETNAME`
- `SELECT 0` - only db index zero is allowed
- `DBSIZE`
- `FLUSHALL`
- `FLUSHDB` - same as `FLUSHALL` as there is only one db
- `SET key value [ EX ttl_seconds | PX ttl_milliseconds ] [ NX | XX ]`
- `SETEX key ttl_seconds value`
- `PSETEX key ttl_milliseconds value`
- `MSET key value [ key value ... ]`
- `MSETNX key value [ key value ... ]`
- `EXISTS key`
- `TYPE key` - always returns `string` for existing keys
- `GET key`
- `MGET key [ key ... ]`
- `EXPIRE key ttl_seconds`
- `PEXPIRE key ttl_milliseconds`
- `EXPIREAT key unix_timestamp`
- `TTL key`
- `PTTL key`
- `DEL key`
- `KEYS pattern`
- `SCAN cursor [ COUNT count ] [ MATCH pattern ]`
- `QUIT`
- `SAVE`
- `DUMPALL` - outputs SET commands that would recreate the current dataset - used for persisting data

## Auto-persist
The server auto saves the current dataset to a file called persist.db.
This can be disabled with the `-n` or `--no-save` command line option

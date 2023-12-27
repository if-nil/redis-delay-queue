# redis_delay_queue

## Overview

`redis_delay_queue` is a [redis module](https://redis.io/docs/reference/modules/) that implements a delay queue. 

You can push a message and receive a message id, which can be received in the list or pub/sub after `delay_time` seconds.

## Build

The rust runtime environment is required: https://www.rust-lang.org/tools/install

Use cargo to build releases:

``` bash
cargo build --release
```

## Run

### Server
``` bash
redis-server --loadmodule ./target/release/libredis_delay_queue.so
```

### Client
``` bash
127.0.0.1:6379> MODULE LOAD /your/path/to/libredis_delay_queue.so
```

## Commands

``` shell
delay_queue.push queue_name message delay_time mode
```
- **delay_time:** after `delay_time` seconds, the message is sent to the corresponding queue_name
- **mode:** `p2p` or `broadcast`, send to list or pub/sub

### example


### p2p

```
127.0.0.1:6379> delay_queue.push my_queue my_message 5 p2p
"5e355e95e7d84c22b94bade9a28788c6"
```

```
127.0.0.1:6379> LRANGE my_queue 0 10
1) "{\"id\":\"5e355e95e7d84c22b94bade9a28788c6\",\"queue_name\":\"my_queue\",\"msg\":\"my_message\",\"delay_time\":{\"secs_since_epoch\":1703684571,\"nanos_since_epoch\":192411990},\"mode\":\"P2P\"}"
```

Explanation: after 5 seconds, you can read the `my_message` inside the list named `my_queue`


#### broadcast
```
127.0.0.1:6379> delay_queue.push my_queue my_message 5 broadcast
"aa6656ba5b57404aa2c74a27ba89d6f1"
```
```
127.0.0.1:6379> SUBSCRIBE my_queue
1) "subscribe"
2) "my_queue"
3) (integer) 1
Reading messages... (press Ctrl-C to quit or any key to type
1) "message"
2) "my_queue"
3) "{\"id\":\"aa6656ba5b57404aa2c74a27ba89d6f1\",\"queue_name\":\"my_queue\",\"msg\":\"my_message\",\"delay_time\":{\"secs_since_epoch\":1703684786,\"nanos_since_epoch\":334841998},\"mode\":\"Broadcast\"}"
```
Explanation: If you are using `SUBSCRIBE my_queue` to listen for channel messages, then after 5s you can receive `my_message`

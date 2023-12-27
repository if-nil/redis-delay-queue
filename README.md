## 测试

启动一个临时redis容器并load module
``` shell
docker run --rm --name redis-demo -v /home/baba/Coding/rust/delay-queue/target/debug/libredis_delay_queue.so:/libredis_delay_queue.so redis redis-server  --enable-module-command local --loadmodule /libredis_delay_queue.so
```

打开redis-cli
``` shell
docker exec -it redis-demo redis-cli
```
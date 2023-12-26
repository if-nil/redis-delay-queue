## 测试

启动一个临时redis容器
``` shell
docker run --rm --name redis-demo -v /home/baba/Coding/rust/delay-queue/target/debug/libredis_delay_queue.so:/libredis_delay_queue.so redis redis-server  --enable-module-command local --loadmodule /libredis_delay_queue.so
```

打开redis-cli并load module
``` shell
docker exec -it redis-demo redis-cli
```
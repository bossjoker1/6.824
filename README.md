# 6.824
MIT 6.824 try to work it out!

## lab1
运行命令：
```shell
go build -race -buildmode=plugin ../mrapps/wc.go
rm mr-out*
go run -race mrcoordinator.go pg-*.txt
# another terminal
go run -race mrworker.go wc.so
```
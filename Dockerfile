
FROM golang:1.15 as builder
ENV GOPROXY https://goproxy.io
ENV GO111MODULE on
WORKDIR /usr/local/go/src/TaskStatusTracker
ADD ./go.mod .
ADD ./go.sum .
RUN go mod download
ADD .  /usr/local/go/src/TaskStatusTracker/
WORKDIR /usr/local/go/src/TaskStatusTracker

#go构建可执行文件,-o 生成Server，放在当前目录
#RUN GOOS=linux GOARCH=amd64  GO111MODULE=on go build -ldflags="-w -s" -o server .
RUN go build -ldflags="-w -s" -o TaskStatusTracker .

FROM ubuntu:latest
WORKDIR /root/go/TaskStatusTracker
COPY --from=builder /usr/local/go/src/TaskStatusTracker .
ENTRYPOINT  ["./TaskStatusTracker"]


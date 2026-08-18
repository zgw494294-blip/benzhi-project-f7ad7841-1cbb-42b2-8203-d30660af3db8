# BENZHI_README

## 项目说明
- 项目：benzhi-project-f7ad7841-1cbb-42b2-8203-d30660af3db8
- 项目用途：已完成样链采样封签交接账本的分层 Go 服务、JSON HTTP API、本地原子持久化、完整业务测试和中文项目说明，并通过 go test ./... 与 go run ./cmd/selfcheck。
- Go 工具链：`golang:1.22`
- 前端工具链：无

## 标准构建、运行和测试命令
进入容器后执行：
cd '/app' && GOTOOLCHAIN=local go build ./...
cd /app && GOTOOLCHAIN=local go run ./cmd/selfcheck
cd '/app' && GOTOOLCHAIN=local go test ./...

## Docker 构建和进入容器
chmod +x build_benzhi_docker.sh
./build_benzhi_docker.sh benzhi-project-f7ad7841-1cbb-42b2-8203-d30660af3db8-amd64 linux/amd64
./build_benzhi_docker.sh benzhi-project-f7ad7841-1cbb-42b2-8203-d30660af3db8-arm64 linux/arm64
docker run -it benzhi-project-f7ad7841-1cbb-42b2-8203-d30660af3db8-amd64:latest

## 题目验证命令
1. 预期退出码 0：`go test ./...`
2. 预期退出码 0：`go run ./cmd/selfcheck`

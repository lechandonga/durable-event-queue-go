.PHONY: test test-race build vet fmt fmt-check

# 一条命令跑通全部本地测试（并发生产 / 强杀重启 / 重复消费，无任何外部依赖）
test:
	go test ./... -count=1 -v

# 带竞态检测的完整测试
test-race:
	CGO_ENABLED=1 go test ./... -count=1 -race

build:
	go build ./...

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l .; exit 1)

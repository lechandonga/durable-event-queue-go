.PHONY: test test-race vet fmt check clean

# 本地一条命令跑通全部测试（并发生产 / 强杀重启 / 重复消费），
# 不依赖任何外部服务或真实凭据。
test:
	go test -timeout 120s -v ./...

# 竞态检测（需要 gcc）。
test-race:
	CGO_ENABLED=1 go test -race -timeout 180s -v ./...

vet:
	go vet ./...

fmt:
	gofmt -l -w .

check: vet test-race

clean:
	rm -rf ./data

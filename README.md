# 样链采样封签交接账本

样链采样封签交接账本是一个使用 Go 标准库实现的本地 JSON HTTP 服务，用于记录实验室采样容器从建批、封签发运、运输交接到接收关闭的完整保管链，并生成可复核的交接凭据。

## 标准操作

构建生产服务：

```text
go build ./cmd/samplechain
```

启动服务（默认监听 `:8080`，账本写入 `samplechain.json`）：

```text
go run ./cmd/samplechain
```

运行进程内完整流程自检：

```text
go run ./cmd/selfcheck
```

运行全部测试：

```text
go test ./...
```

服务提供批次创建、发运前清单修订、发运、运输交接、逐容器接收、批量接收、关闭和单批查询接口。`PUT /v1/batches/{id}/containers/{containerID}/receipt` 支持按拆箱顺序逐件登记封签与温度核验结果，最后一件提交后自动完成接收；批量接收接口仍可一次提交完整结果。`GET /v1/batches/{id}` 返回验收进度、已提交数量、总数、待验收容器和 `lifecycleEvents` 生命周期审计时间线，事件按序记录创建、清单修订、发运、完成接收和关闭及其提交版本与 UTC 时间。`GET /v1/batches` 支持按状态、目的地和容器条件筛选，并使用 `limit` 与 `cursor` 分页返回批次摘要，同时返回基于完整过滤结果的批次、容器、交接和状态条件汇总；`GET /v1/batches/{id}/receipt/verification` 可只读核验关闭凭据。所有状态变化都带有版本校验，运输交接支持幂等重试和责任链连续校验；本地账本使用带格式版本的 JSON 文件保存，并通过临时文件和原子替换提交快照。

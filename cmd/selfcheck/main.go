package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"samplechain/internal/app"
	"samplechain/internal/domain"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "流程自检失败:", err)
		os.Exit(1)
	}
	fmt.Println("流程自检通过：批次已完成接收、关闭并生成凭据")
}

func run() error {
	directory, err := os.MkdirTemp("", "samplechain-selfcheck-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(directory)
	application, err := app.New(app.Config{Address: "127.0.0.1:0", LedgerPath: filepath.Join(directory, "ledger.json")})
	if err != nil {
		return err
	}
	defer application.Close()
	server := httptest.NewServer(application.Handler())
	defer server.Close()
	client := server.Client()
	when := time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC)
	create := map[string]any{"id": "SC-DEMO-001", "destination": "中心实验室", "responsiblePerson": "李晨", "expectedVersion": 0, "containers": []any{
		map[string]any{"containerID": "C-01", "sampleLabel": "血清-A", "sealNumber": "SEAL-01", "temperatureMinC": 2, "temperatureMaxC": 8},
		map[string]any{"containerID": "C-02", "sampleLabel": "血清-B", "sealNumber": "SEAL-02", "temperatureMinC": -20, "temperatureMaxC": -10},
	}}
	var batch domain.CustodyBatch
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches", create, http.StatusCreated, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusDraft || batch.Version != 1 {
		return fmt.Errorf("创建结果不正确")
	}
	if err := checkLifecycle(batch, []domain.LifecycleEventType{domain.LifecycleCreated}, []uint64{1}); err != nil {
		return err
	}
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/dispatch", map[string]any{"expectedVersion": 0}, http.StatusConflict, nil); err != nil {
		return err
	}
	manifest := map[string]any{"expectedVersion": 1, "destination": "中心实验室", "responsiblePerson": "李晨", "containers": create["containers"]}
	if err := requestJSON(client, http.MethodPut, server.URL+"/v1/batches/SC-DEMO-001/manifest", manifest, http.StatusOK, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusDraft || batch.Version != 2 {
		return fmt.Errorf("清单修订结果不正确")
	}
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/dispatch", map[string]any{"expectedVersion": 2}, http.StatusOK, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusInTransit || batch.Version != 3 {
		return fmt.Errorf("发运结果不正确")
	}
	if err := checkLifecycle(batch, []domain.LifecycleEventType{domain.LifecycleCreated, domain.LifecycleManifestUpdated, domain.LifecycleDispatched}, []uint64{1, 2, 3}); err != nil {
		return err
	}
	handoff := map[string]any{"expectedVersion": 3, "eventID": "EV-01", "idempotencyKey": "handoff-01", "sequence": 1, "fromPerson": "李晨", "toPerson": "周宁", "location": "冷链站", "occurredAt": when}
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/handoffs", handoff, http.StatusOK, &batch); err != nil {
		return err
	}
	if len(batch.Handoffs) != 1 || batch.Version != 4 {
		return fmt.Errorf("交接结果不正确")
	}
	handoff["expectedVersion"] = 4
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/handoffs", handoff, http.StatusOK, &batch); err != nil {
		return err
	}
	if len(batch.Handoffs) != 1 || batch.Version != 4 {
		return fmt.Errorf("幂等重试改变了交接历史")
	}
	firstReceipt := map[string]any{"expectedVersion": 4, "receivedSealNumber": "SEAL-01", "receivedTemperatureC": 8}
	if err := requestJSON(client, http.MethodPut, server.URL+"/v1/batches/SC-DEMO-001/containers/C-01/receipt", firstReceipt, http.StatusOK, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusInTransit || batch.Version != 5 || batch.ReceiptProgress == nil || batch.ReceiptProgress.SubmittedCount != 1 || batch.ReceiptProgress.TotalCount != 2 || len(batch.ReceiptProgress.PendingContainerIDs) != 1 || batch.ReceiptProgress.PendingContainerIDs[0] != "C-02" {
		return fmt.Errorf("分阶段接收进度不正确")
	}
	if err := checkLifecycle(batch, []domain.LifecycleEventType{domain.LifecycleCreated, domain.LifecycleManifestUpdated, domain.LifecycleDispatched}, []uint64{1, 2, 3}); err != nil {
		return err
	}
	secondCreate := map[string]any{"id": "SC-DRAFT-002", "destination": "中心实验室", "responsiblePerson": "王宁", "expectedVersion": 0, "containers": []any{
		map[string]any{"containerID": "C-03", "sampleLabel": "血浆-C", "sealNumber": "SEAL-03", "temperatureMinC": 2, "temperatureMaxC": 8},
		map[string]any{"containerID": "C-04", "sampleLabel": "血浆-D", "sealNumber": "SEAL-04", "temperatureMinC": 2, "temperatureMaxC": 8},
	}}
	var secondBatch domain.CustodyBatch
	if err := requestJSON(client, http.MethodPost, server.URL+"/v1/batches", secondCreate, http.StatusCreated, &secondBatch); err != nil {
		return err
	}
	server.Close()
	if err := application.Close(); err != nil {
		return err
	}
	application, err = app.New(app.Config{Address: "127.0.0.1:0", LedgerPath: filepath.Join(directory, "ledger.json")})
	if err != nil {
		return err
	}
	defer application.Close()
	server = httptest.NewServer(application.Handler())
	defer server.Close()
	var reloaded domain.CustodyBatch
	if err := requestJSON(server.Client(), http.MethodGet, server.URL+"/v1/batches/SC-DEMO-001", nil, http.StatusOK, &reloaded); err != nil {
		return err
	}
	if reloaded.Status != domain.StatusInTransit || reloaded.Version != 5 || reloaded.ReceiptProgress == nil || reloaded.ReceiptProgress.SubmittedCount != 1 || reloaded.ReceiptProgress.PendingContainerIDs[0] != "C-02" {
		return fmt.Errorf("重启后验收进度不正确")
	}
	if err := checkLifecycle(reloaded, []domain.LifecycleEventType{domain.LifecycleCreated, domain.LifecycleManifestUpdated, domain.LifecycleDispatched}, []uint64{1, 2, 3}); err != nil {
		return err
	}
	var firstPage domain.BatchListResult
	if err := requestJSON(server.Client(), http.MethodGet, server.URL+"/v1/batches?limit=1", nil, http.StatusOK, &firstPage); err != nil {
		return err
	}
	if len(firstPage.Items) != 1 || firstPage.NextCursor == "" || firstPage.Totals.BatchCount != 2 || firstPage.Totals.ContainerCount != 4 || firstPage.Totals.HandoffCount != 1 || firstPage.Totals.StatusCounts[domain.StatusDraft] != 1 || firstPage.Totals.StatusCounts[domain.StatusInTransit] != 1 || firstPage.Totals.ConditionCounts[domain.ConditionPending] != 3 || firstPage.Totals.ConditionCounts[domain.ConditionNormal] != 1 || firstPage.Totals.ConditionCounts[domain.ConditionException] != 0 {
		return fmt.Errorf("批次汇总或分页首页不正确")
	}
	var secondPage domain.BatchListResult
	if err := requestJSON(server.Client(), http.MethodGet, server.URL+"/v1/batches?limit=1&cursor="+url.QueryEscape(firstPage.NextCursor), nil, http.StatusOK, &secondPage); err != nil {
		return err
	}
	if len(secondPage.Items) != 1 || secondPage.NextCursor != "" || !reflect.DeepEqual(firstPage.Totals, secondPage.Totals) {
		return fmt.Errorf("批次汇总跨页不一致")
	}
	secondReceipt := map[string]any{"expectedVersion": 5, "receivedSealNumber": "SEAL-X", "receivedTemperatureC": -5, "exceptionNote": "封签不一致且温度偏高"}
	if err := requestJSON(server.Client(), http.MethodPut, server.URL+"/v1/batches/SC-DEMO-001/containers/C-02/receipt", secondReceipt, http.StatusOK, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusReceived || batch.Version != 6 || batch.Containers[0].Condition != domain.ConditionNormal || batch.Containers[1].Condition != domain.ConditionException || batch.ReceiptProgress == nil || len(batch.ReceiptProgress.PendingContainerIDs) != 0 {
		return fmt.Errorf("最终接收结果不正确")
	}
	if err := requestJSON(server.Client(), http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/close", map[string]any{"expectedVersion": 6}, http.StatusOK, &batch); err != nil {
		return err
	}
	if batch.Status != domain.StatusClosed || batch.Version != 7 || batch.Receipt == nil || batch.Receipt.Digest == "" {
		return fmt.Errorf("关闭凭据不正确")
	}
	if err := checkLifecycle(batch, []domain.LifecycleEventType{domain.LifecycleCreated, domain.LifecycleManifestUpdated, domain.LifecycleDispatched, domain.LifecycleReceived, domain.LifecycleClosed}, []uint64{1, 2, 3, 6, 7}); err != nil {
		return err
	}
	var queried domain.CustodyBatch
	if err := requestJSON(client, http.MethodGet, server.URL+"/v1/batches/SC-DEMO-001", nil, http.StatusOK, &queried); err != nil {
		return err
	}
	if queried.Receipt == nil || queried.Receipt.Digest != batch.Receipt.Digest || queried.Version != 7 {
		return fmt.Errorf("查询结果不正确")
	}
	var verification domain.ReceiptVerification
	if err := requestJSON(server.Client(), http.MethodGet, server.URL+"/v1/batches/SC-DEMO-001/receipt/verification", nil, http.StatusOK, &verification); err != nil {
		return err
	}
	if !verification.Valid {
		return fmt.Errorf("关闭凭据核验不正确")
	}
	if err := requestJSON(server.Client(), http.MethodPost, server.URL+"/v1/batches/SC-DEMO-001/dispatch", map[string]any{"expectedVersion": 7}, http.StatusUnprocessableEntity, nil); err != nil {
		return err
	}
	if err := checkLifecycle(queried, []domain.LifecycleEventType{domain.LifecycleCreated, domain.LifecycleManifestUpdated, domain.LifecycleDispatched, domain.LifecycleReceived, domain.LifecycleClosed}, []uint64{1, 2, 3, 6, 7}); err != nil {
		return err
	}
	var filtered domain.BatchListResult
	if err := requestJSON(server.Client(), http.MethodGet, server.URL+"/v1/batches?status=CLOSED&destination="+url.QueryEscape("中心实验室")+"&condition=EXCEPTION&limit=1", nil, http.StatusOK, &filtered); err != nil {
		return err
	}
	if filtered.Totals.BatchCount != 1 || filtered.Totals.ContainerCount != 2 || filtered.Totals.ConditionCounts[domain.ConditionNormal] != 1 || filtered.Totals.ConditionCounts[domain.ConditionException] != 1 || filtered.Totals.StatusCounts[domain.StatusClosed] != 1 {
		return fmt.Errorf("过滤汇总不正确")
	}
	return nil
}

func checkLifecycle(batch domain.CustodyBatch, types []domain.LifecycleEventType, versions []uint64) error {
	if len(batch.LifecycleEvents) != len(types) || len(types) != len(versions) {
		return fmt.Errorf("生命周期事件数量不正确")
	}
	for index, event := range batch.LifecycleEvents {
		if event.Sequence != uint64(index+1) || event.Type != types[index] || event.Version != versions[index] || event.OccurredAt.IsZero() || event.OccurredAt.Location() != time.UTC {
			return fmt.Errorf("生命周期事件顺序或版本不正确")
		}
	}
	return nil
}

func requestJSON(client *http.Client, method, endpoint string, input any, expectedStatus int, output any) error {
	var body io.Reader
	if input != nil {
		data, err := json.Marshal(input)
		if err != nil {
			return err
		}
		body = bytes.NewReader(data)
	}
	request, err := http.NewRequest(method, endpoint, body)
	if err != nil {
		return err
	}
	if input != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != expectedStatus {
		data, _ := io.ReadAll(response.Body)
		return fmt.Errorf("%s %s 返回 %d，期望 %d：%s", method, endpoint, response.StatusCode, expectedStatus, string(data))
	}
	if output != nil {
		if err := json.NewDecoder(response.Body).Decode(output); err != nil {
			return err
		}
	}
	return nil
}

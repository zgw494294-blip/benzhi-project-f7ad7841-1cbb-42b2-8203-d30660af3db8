package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"samplechain/internal/custody"
	"samplechain/internal/domain"
	"samplechain/internal/ledger"
)

func testHTTPHandler(t *testing.T) http.Handler {
	t.Helper()
	store, err := ledger.OpenJSON(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := custody.NewService(store)
	if err != nil {
		store.Close()
		t.Fatal(err)
	}
	handler, err := NewHandler(service)
	if err != nil {
		service.Close()
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return handler
}

func TestHandlerStrictJSONAndRequiredVersion(t *testing.T) {
	server := httptest.NewServer(testHTTPHandler(t))
	defer server.Close()
	client := server.Client()
	valid := `{"id":"B-HTTP","destination":"实验室","responsiblePerson":"甲","expectedVersion":0,"containers":[{"containerID":"C-01","sampleLabel":"样品","sealNumber":"S-01","temperatureMinC":0,"temperatureMaxC":10}]}`
	response := postRaw(t, client, server.URL+"/v1/batches", valid+" "+valid)
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("trailing JSON status = %d", response.StatusCode)
	}
	response = postRaw(t, client, server.URL+"/v1/batches", `{"id":"B-HTTP","destination":"实验室","responsiblePerson":"甲","expectedVersion":0,"unknown":true,"containers":[]}`)
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("unknown field status = %d", response.StatusCode)
	}
	response = postRaw(t, client, server.URL+"/v1/batches", strings.Replace(valid, `,"expectedVersion":0`, "", 1))
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing version status = %d", response.StatusCode)
	}
}

func TestHandlerRejectsOversizedBodyAndReturnsJSONErrors(t *testing.T) {
	server := httptest.NewServer(testHTTPHandler(t))
	defer server.Close()
	client := server.Client()
	payload := `{"expectedVersion":0,"id":"B-LARGE","destination":"实验室","responsiblePerson":"甲","containers":[{"containerID":"C-01","sampleLabel":"` + strings.Repeat("x", maxRequestBody) + `","sealNumber":"S-01","temperatureMinC":0,"temperatureMaxC":10}]}`
	response := postRaw(t, client, server.URL+"/v1/batches", payload)
	defer response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatalf("oversized body status = %d", response.StatusCode)
	}
	var envelope map[string]any
	if err := json.NewDecoder(response.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if _, ok := envelope["error"]; !ok {
		t.Fatalf("error response = %#v", envelope)
	}
}

func TestHandlerFullWorkflow(t *testing.T) {
	server := httptest.NewServer(testHTTPHandler(t))
	defer server.Close()
	client := server.Client()
	create := map[string]any{"id": "B-FLOW", "destination": "实验室", "responsiblePerson": "甲", "expectedVersion": 0, "containers": []any{map[string]any{"containerID": "C-01", "sampleLabel": "样品", "sealNumber": "S-01", "temperatureMinC": 2, "temperatureMaxC": 8}}}
	var batch domain.CustodyBatch
	if err := doJSON(t, client, http.MethodPost, server.URL+"/v1/batches", create, http.StatusCreated, &batch); err != nil {
		t.Fatal(err)
	}
	if err := doJSON(t, client, http.MethodPost, server.URL+"/v1/batches/B-FLOW/dispatch", map[string]any{"expectedVersion": 1}, http.StatusOK, &batch); err != nil {
		t.Fatal(err)
	}
	if err := doJSON(t, client, http.MethodPost, server.URL+"/v1/batches/B-FLOW/receive", map[string]any{"expectedVersion": 2, "results": []any{map[string]any{"containerID": "C-01", "receivedSealNumber": "S-01", "receivedTemperatureC": 2}}}, http.StatusOK, &batch); err != nil {
		t.Fatal(err)
	}
	if err := doJSON(t, client, http.MethodPost, server.URL+"/v1/batches/B-FLOW/close", map[string]any{"expectedVersion": 3}, http.StatusOK, &batch); err != nil {
		t.Fatal(err)
	}
	if batch.Status != domain.StatusClosed || batch.Receipt == nil {
		t.Fatalf("workflow result = %+v", batch)
	}
}

func postRaw(t *testing.T, client *http.Client, endpoint, body string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(http.MethodPost, endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	return response
}

func doJSON(t *testing.T, client *http.Client, method, endpoint string, input any, status int, output any) error {
	t.Helper()
	data, err := json.Marshal(input)
	if err != nil {
		return err
	}
	request, err := http.NewRequest(method, endpoint, bytes.NewReader(data))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode != status {
		body, _ := io.ReadAll(response.Body)
		return &unexpectedStatus{got: response.StatusCode, want: status, body: string(body)}
	}
	return json.NewDecoder(response.Body).Decode(output)
}

type unexpectedStatus struct {
	got  int
	want int
	body string
}

func (e *unexpectedStatus) Error() string { return "unexpected HTTP status" }

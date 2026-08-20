package http_context_error_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"samplechain/internal/domain"
	"samplechain/internal/httpapi"
)

type canceledService struct{ httpapi.Service }

func (canceledService) GetBatch(context.Context, string) (domain.CustodyBatch, error) {
	return domain.CustodyBatch{}, context.Canceled
}

func TestCanceledServiceErrorUsesRequestTimeoutStatus(t *testing.T) {
	handler, err := httpapi.NewHandler(canceledService{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodGet, "http://example.test/v1/batches/B-CANCELED", nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusRequestTimeout {
		t.Fatalf("请求取消错误返回了错误的 HTTP 状态码：%d", response.Code)
	}
}

package http_body_close_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"samplechain/internal/custody"
	"samplechain/internal/httpapi"
	"samplechain/internal/ledger"
)

type trackingBody struct{ closed bool }

func (*trackingBody) Read([]byte) (int, error) { return 0, io.EOF }
func (b *trackingBody) Close() error           { b.closed = true; return nil }

func TestServeHTTPClosesRequestBodyOnRoutingFailure(t *testing.T) {
	store, err := ledger.OpenJSON(filepath.Join(t.TempDir(), "ledger.json"))
	if err != nil {
		t.Fatal(err)
	}
	service, err := custody.NewService(store)
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	handler, err := httpapi.NewHandler(service)
	if err != nil {
		t.Fatal(err)
	}
	body := &trackingBody{}
	request := httptest.NewRequest(http.MethodGet, "http://example.test/not-found", body)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if !body.closed {
		t.Fatal("路由失败后请求体未关闭")
	}
}

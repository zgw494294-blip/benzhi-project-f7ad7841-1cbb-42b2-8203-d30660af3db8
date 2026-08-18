package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"samplechain/internal/custody"
	"samplechain/internal/domain"
	"samplechain/internal/ledger"
)

const maxRequestBody = 1 << 20

type Service interface {
	CreateBatch(context.Context, domain.NewBatchInput, uint64) (domain.CustodyBatch, error)
	Dispatch(context.Context, string, uint64) (domain.CustodyBatch, error)
	UpdateManifest(context.Context, string, uint64, domain.ManifestInput) (domain.CustodyBatch, error)
	AddHandoff(context.Context, string, uint64, domain.HandoffInput) (domain.CustodyBatch, error)
	Receive(context.Context, string, uint64, domain.ReceiveInput) (domain.CustodyBatch, error)
	StageReceipt(context.Context, string, string, uint64, domain.ReceiveResult) (domain.CustodyBatch, error)
	CloseBatch(context.Context, string, uint64) (domain.CustodyBatch, error)
	GetBatch(context.Context, string) (domain.CustodyBatch, error)
	ListBatches(context.Context, domain.BatchListQuery) (domain.BatchListResult, error)
	VerifyReceipt(context.Context, string) (domain.ReceiptVerification, error)
}

type Handler struct {
	service Service
}

func NewHandler(service Service) (*Handler, error) {
	if service == nil {
		return nil, errors.New("业务服务不能为空")
	}
	return &Handler{service: service}, nil
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer closeRequestBody(r.Body)
	if r.URL.Path == "/v1/batches" {
		switch r.Method {
		case http.MethodPost:
			h.create(w, r)
			return
		case http.MethodGet:
			h.list(w, r)
			return
		}
	}
	parts := pathParts(r.URL.Path)
	if len(parts) < 3 || len(parts) > 6 || parts[0] != "v1" || parts[1] != "batches" {
		writeError(w, http.StatusNotFound, "not_found", "接口不存在")
		return
	}
	id, err := url.PathUnescape(parts[2])
	if err != nil || id == "" || strings.ContainsRune(id, '/') {
		writeError(w, http.StatusBadRequest, "invalid_id", "批次编号无效")
		return
	}
	if len(parts) == 3 && r.Method == http.MethodGet {
		h.get(w, r, id)
		return
	}
	if len(parts) == 4 && parts[3] == "manifest" && r.Method == http.MethodPut {
		h.updateManifest(w, r, id)
		return
	}
	if len(parts) == 4 && parts[3] == "dispatch" && r.Method == http.MethodPost {
		h.dispatch(w, r, id)
		return
	}
	if len(parts) == 4 && parts[3] == "handoffs" && r.Method == http.MethodPost {
		h.handoff(w, r, id)
		return
	}
	if len(parts) == 4 && parts[3] == "receive" && r.Method == http.MethodPost {
		h.receive(w, r, id)
		return
	}
	if len(parts) == 6 && parts[3] == "containers" && parts[5] == "receipt" && r.Method == http.MethodPut {
		containerID, err := url.PathUnescape(parts[4])
		if err != nil || containerID == "" || strings.ContainsRune(containerID, '/') || strings.ContainsRune(containerID, '\x00') {
			writeError(w, http.StatusBadRequest, "invalid_id", "容器编号无效")
			return
		}
		h.stageReceipt(w, r, id, containerID)
		return
	}
	if len(parts) == 4 && parts[3] == "close" && r.Method == http.MethodPost {
		h.closeBatch(w, r, id)
		return
	}
	if len(parts) == 5 && parts[3] == "receipt" && parts[4] == "verification" && r.Method == http.MethodGet {
		h.verifyReceipt(w, r, id)
		return
	}
	writeError(w, http.StatusNotFound, "not_found", "接口不存在")
}

func (h *Handler) create(w http.ResponseWriter, r *http.Request) {
	var request createRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	containers := make([]domain.ContainerInput, len(request.Containers))
	for index, item := range request.Containers {
		containers[index] = domain.ContainerInput{ContainerID: item.ContainerID, SampleLabel: item.SampleLabel, SealNumber: item.SealNumber, TemperatureMinC: item.TemperatureMinC, TemperatureMaxC: item.TemperatureMaxC}
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.CreateBatch(r.Context(), domain.NewBatchInput{ID: request.ID, Destination: request.Destination, ResponsiblePerson: request.ResponsiblePerson, Containers: containers}, *request.ExpectedVersion)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (h *Handler) dispatch(w http.ResponseWriter, r *http.Request, id string) {
	var request versionRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.Dispatch(r.Context(), id, *request.ExpectedVersion)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) updateManifest(w http.ResponseWriter, r *http.Request, id string) {
	var request manifestRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	containers := make([]domain.ContainerInput, len(request.Containers))
	for index, item := range request.Containers {
		containers[index] = domain.ContainerInput{ContainerID: item.ContainerID, SampleLabel: item.SampleLabel, SealNumber: item.SealNumber, TemperatureMinC: item.TemperatureMinC, TemperatureMaxC: item.TemperatureMaxC}
	}
	result, err := h.service.UpdateManifest(r.Context(), id, *request.ExpectedVersion, domain.ManifestInput{Destination: request.Destination, ResponsiblePerson: request.ResponsiblePerson, Containers: containers})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) handoff(w http.ResponseWriter, r *http.Request, id string) {
	var request handoffRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.AddHandoff(r.Context(), id, *request.ExpectedVersion, domain.HandoffInput{EventID: request.EventID, IdempotencyKey: request.IdempotencyKey, Sequence: request.Sequence, FromPerson: request.FromPerson, ToPerson: request.ToPerson, Location: request.Location, OccurredAt: request.OccurredAt})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) receive(w http.ResponseWriter, r *http.Request, id string) {
	var request receiveRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	results := make([]domain.ReceiveResult, len(request.Results))
	for index, item := range request.Results {
		results[index] = domain.ReceiveResult{ContainerID: item.ContainerID, ReceivedSealNumber: item.ReceivedSealNumber, ReceivedTemperatureC: item.ReceivedTemperatureC, ExceptionNote: item.ExceptionNote}
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.Receive(r.Context(), id, *request.ExpectedVersion, domain.ReceiveInput{Results: results})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) stageReceipt(w http.ResponseWriter, r *http.Request, id, containerID string) {
	var request receiptRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.StageReceipt(r.Context(), id, containerID, *request.ExpectedVersion, domain.ReceiveResult{
		ReceivedSealNumber: request.ReceivedSealNumber, ReceivedTemperatureC: request.ReceivedTemperatureC, ExceptionNote: request.ExceptionNote,
	})
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) closeBatch(w http.ResponseWriter, r *http.Request, id string) {
	var request versionRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.ExpectedVersion == nil {
		writeError(w, http.StatusBadRequest, "missing_version", "写请求必须提供 expectedVersion")
		return
	}
	result, err := h.service.CloseBatch(r.Context(), id, *request.ExpectedVersion)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) get(w http.ResponseWriter, r *http.Request, id string) {
	result, err := h.service.GetBatch(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) list(w http.ResponseWriter, r *http.Request) {
	query, err := parseBatchListQuery(r.URL.Query())
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_query", "查询参数无效")
		return
	}
	result, err := h.service.ListBatches(r.Context(), query)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) verifyReceipt(w http.ResponseWriter, r *http.Request, id string) {
	result, err := h.service.VerifyReceipt(r.Context(), id)
	if err != nil {
		writeServiceError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

type createRequest struct {
	ID                string             `json:"id"`
	Destination       string             `json:"destination"`
	ResponsiblePerson string             `json:"responsiblePerson"`
	Containers        []containerRequest `json:"containers"`
	ExpectedVersion   *uint64            `json:"expectedVersion"`
}

type manifestRequest struct {
	Destination       string             `json:"destination"`
	ResponsiblePerson string             `json:"responsiblePerson"`
	Containers        []containerRequest `json:"containers"`
	ExpectedVersion   *uint64            `json:"expectedVersion"`
}

type containerRequest struct {
	ContainerID     string  `json:"containerID"`
	SampleLabel     string  `json:"sampleLabel"`
	SealNumber      string  `json:"sealNumber"`
	TemperatureMinC float64 `json:"temperatureMinC"`
	TemperatureMaxC float64 `json:"temperatureMaxC"`
}

type versionRequest struct {
	ExpectedVersion *uint64 `json:"expectedVersion"`
}

type handoffRequest struct {
	ExpectedVersion *uint64   `json:"expectedVersion"`
	EventID         string    `json:"eventID"`
	IdempotencyKey  string    `json:"idempotencyKey"`
	Sequence        uint64    `json:"sequence"`
	FromPerson      string    `json:"fromPerson"`
	ToPerson        string    `json:"toPerson"`
	Location        string    `json:"location"`
	OccurredAt      time.Time `json:"occurredAt"`
}

type receiveRequest struct {
	ExpectedVersion *uint64         `json:"expectedVersion"`
	Results         []receiveResult `json:"results"`
}

type receiveResult struct {
	ContainerID          string  `json:"containerID"`
	ReceivedSealNumber   string  `json:"receivedSealNumber"`
	ReceivedTemperatureC float64 `json:"receivedTemperatureC"`
	ExceptionNote        string  `json:"exceptionNote"`
}

type receiptRequest struct {
	ExpectedVersion      *uint64 `json:"expectedVersion"`
	ReceivedSealNumber   string  `json:"receivedSealNumber"`
	ReceivedTemperatureC float64 `json:"receivedTemperatureC"`
	ExceptionNote        string  `json:"exceptionNote"`
}

func decodeJSON(w http.ResponseWriter, r *http.Request, target any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, maxRequestBody)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		message := "请求 JSON 无效"
		if errors.Is(err, io.EOF) {
			message = "请求体不能为空"
		} else if strings.Contains(err.Error(), "request body too large") {
			message = "请求体超过大小限制"
		}
		writeError(w, http.StatusBadRequest, "invalid_json", message)
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		writeError(w, http.StatusBadRequest, "invalid_json", "请求体只能包含一个 JSON 对象")
		return false
	}
	return true
}

func writeServiceError(w http.ResponseWriter, err error) {
	status, code, message := http.StatusInternalServerError, "internal_error", "服务暂时无法完成请求"
	switch {
	case errors.Is(err, domain.ErrInvalidQuery):
		status, code, message = http.StatusBadRequest, "invalid_query", "查询参数无效"
	case errors.Is(err, ledger.ErrNotFound):
		status, code, message = http.StatusNotFound, "not_found", "批次不存在"
	case errors.Is(err, custody.ErrVersionConflict), errors.Is(err, ledger.ErrVersionConflict):
		status, code, message = http.StatusConflict, "version_conflict", "批次版本已变化，请重新读取后重试"
	case errors.Is(err, custody.ErrIdempotencyConflict):
		status, code, message = http.StatusConflict, "idempotency_conflict", "幂等键对应的交接内容不一致"
	case errors.Is(err, domain.ErrInvalidInput), errors.Is(err, domain.ErrInvalidTransition):
		status, code, message = http.StatusUnprocessableEntity, "invalid_request", "请求不符合批次业务规则"
	case isRequestCancellation(err):
		status, code, message = http.StatusRequestTimeout, "request_canceled", "请求已取消"
	case errors.Is(err, ledger.ErrCorruptLedger), errors.Is(err, ledger.ErrUnsupportedFormat):
		status, code, message = http.StatusInternalServerError, "ledger_unavailable", "账本无法读取"
	}
	writeError(w, status, code, message)
}

func parseBatchListQuery(values url.Values) (domain.BatchListQuery, error) {
	query := domain.BatchListQuery{Limit: 20}
	for key, items := range values {
		if len(items) != 1 {
			return domain.BatchListQuery{}, domain.ErrInvalidQuery
		}
		value := strings.TrimSpace(items[0])
		switch key {
		case "status":
			status := domain.BatchStatus(value)
			if value == "" || !isBatchStatus(status) {
				return domain.BatchListQuery{}, domain.ErrInvalidQuery
			}
			query.Status = &status
		case "destination":
			if value == "" {
				return domain.BatchListQuery{}, domain.ErrInvalidQuery
			}
			query.Destination = value
		case "condition":
			condition := domain.ReceiptCondition(value)
			if value == "" || !isReceiptCondition(condition) {
				return domain.BatchListQuery{}, domain.ErrInvalidQuery
			}
			query.Condition = &condition
		case "limit":
			limit, err := strconv.Atoi(value)
			if err != nil {
				return domain.BatchListQuery{}, domain.ErrInvalidQuery
			}
			query.Limit = limit
		case "cursor":
			if value == "" {
				return domain.BatchListQuery{}, domain.ErrInvalidQuery
			}
			query.Cursor = value
		default:
			return domain.BatchListQuery{}, domain.ErrInvalidQuery
		}
	}
	if err := query.Validate(); err != nil {
		return domain.BatchListQuery{}, err
	}
	return query, nil
}

func isBatchStatus(status domain.BatchStatus) bool {
	switch status {
	case domain.StatusDraft, domain.StatusInTransit, domain.StatusReceived, domain.StatusClosed:
		return true
	default:
		return false
	}
}

func isReceiptCondition(condition domain.ReceiptCondition) bool {
	switch condition {
	case domain.ConditionPending, domain.ConditionNormal, domain.ConditionException:
		return true
	default:
		return false
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, map[string]any{"error": map[string]string{"code": code, "message": message}})
}

func closeRequestBody(body io.ReadCloser) {
	if body == nil {
		return
	}
}

func isRequestCancellation(err error) bool {
	if err == nil {
		return false
	}
	return errors.Is(err, context.DeadlineExceeded)
}

func pathParts(path string) []string {
	path = strings.Trim(path, "/")
	if path == "" {
		return nil
	}
	return strings.Split(path, "/")
}

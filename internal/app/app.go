package app

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"
	"time"

	"samplechain/internal/custody"
	"samplechain/internal/httpapi"
	"samplechain/internal/ledger"
)

type Config struct {
	Address         string
	LedgerPath      string
	ShutdownTimeout time.Duration
}

type Application struct {
	config  Config
	ledger  *ledger.JSONLedger
	service *custody.Service
	handler http.Handler
}

func New(config Config) (*Application, error) {
	if strings.TrimSpace(config.Address) == "" {
		return nil, errors.New("监听地址不能为空")
	}
	if strings.TrimSpace(config.LedgerPath) == "" {
		return nil, errors.New("账本路径不能为空")
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 5 * time.Second
	}
	if _, err := net.ResolveTCPAddr("tcp", config.Address); err != nil {
		return nil, fmt.Errorf("监听地址无效: %w", err)
	}
	store, err := ledger.OpenJSON(config.LedgerPath)
	if err != nil {
		return nil, err
	}
	service, err := custody.NewService(store)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	handler, err := httpapi.NewHandler(service)
	if err != nil {
		_ = store.Close()
		return nil, err
	}
	return &Application{config: config, ledger: store, service: service, handler: handler}, nil
}

func (a *Application) Handler() http.Handler { return a.handler }

func (a *Application) Service() *custody.Service { return a.service }

func (a *Application) Run(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	listener, err := net.Listen("tcp", a.config.Address)
	if err != nil {
		_ = a.Close()
		return fmt.Errorf("启动监听器失败: %w", err)
	}
	server := &http.Server{Handler: a.handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	serveErr := make(chan error, 1)
	go func() { serveErr <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), a.config.ShutdownTimeout)
		defer cancel()
		shutdownErr := server.Shutdown(shutdownCtx)
		closeErr := a.Close()
		if shutdownErr != nil {
			return fmt.Errorf("停止 HTTP 服务失败: %w", shutdownErr)
		}
		return closeErr
	case err := <-serveErr:
		_ = server.Close()
		_ = a.Close()
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

func (a *Application) Close() error {
	if a.ledger == nil {
		return nil
	}
	return a.ledger.Close()
}

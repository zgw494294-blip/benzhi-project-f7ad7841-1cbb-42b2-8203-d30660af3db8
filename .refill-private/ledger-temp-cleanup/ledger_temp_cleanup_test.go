package ledger_temp_cleanup_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"samplechain/internal/domain"
	"samplechain/internal/ledger"
)

func TestRenameFailureRemovesTemporaryLedgerFile(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "ledger.json")
	store, err := ledger.OpenJSON(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	batch, err := domain.NewBatch(domain.NewBatchInput{
		ID: "B-TEMP", Destination: "中心实验室", ResponsiblePerson: "甲",
		Containers: []domain.ContainerInput{{ContainerID: "C-01", SampleLabel: "样品", SealNumber: "S-01", TemperatureMinC: 2, TemperatureMaxC: 8}},
	}, time.Date(2026, 8, 18, 9, 0, 0, 0, time.UTC))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Create(context.Background(), batch); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path, 0700); err != nil {
		t.Fatal(err)
	}
	updated := batch.Clone()
	if err := updated.Dispatch(time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC)); err != nil {
		t.Fatal(err)
	}
	if err := store.Commit(context.Background(), batch.ID, batch.Version, updated); err == nil {
		t.Fatal("目标路径为目录时提交竟然成功")
	}
	temporary, err := filepath.Glob(filepath.Join(directory, ".samplechain-*.tmp"))
	if err != nil {
		t.Fatal(err)
	}
	if len(temporary) != 0 {
		t.Fatalf("原子替换失败后遗留临时文件：%v", temporary)
	}
}

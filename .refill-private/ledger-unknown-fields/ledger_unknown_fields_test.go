package ledger_unknown_fields_test

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"samplechain/internal/ledger"
)

func TestLedgerRejectsUnknownSnapshotFields(t *testing.T) {
	path := filepath.Join(t.TempDir(), "ledger.json")
	if err := os.WriteFile(path, []byte(`{"formatVersion":1,"batches":{},"unexpected":true}`), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := ledger.OpenJSON(path); !errors.Is(err, ledger.ErrCorruptLedger) {
		t.Fatalf("账本快照中的未知字段未被拒绝：%v", err)
	}
}

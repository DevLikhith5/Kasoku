package engine

import (
	"fmt"
	"net/http"
	"time"

	"github.com/DevLikhith5/kasoku/internal/config"
	storage "github.com/DevLikhith5/kasoku/internal/store"
	lsmengine "github.com/DevLikhith5/kasoku/internal/store/lsm-engine"
)

type KVEngine interface {
	storage.StorageEngine
	Flush() error
	TriggerCompaction()
}

var (
	cfg       *config.Config
	walSyncMs int
)

func SetConfig(c *config.Config) {
	cfg = c
}

func SetWALSyncMs(ms int) {
	walSyncMs = ms
}

func GetEngine() (KVEngine, error) {
	if cfg == nil {
		return nil, fmt.Errorf("config not loaded")
	}

	// Try remote engine if possible (Cluster NodeAddr is set)
	// We do a quick ping to see if server is alive
	nodeAddr := cfg.Cluster.NodeAddr
	if nodeAddr == "" {
		nodeAddr = fmt.Sprintf("http://localhost:%d", cfg.Port)
	}

	if nodeAddr != "" {
		client := http.Client{Timeout: 100 * time.Millisecond}
		resp, err := client.Get(fmt.Sprintf("%s/health", nodeAddr))
		if err == nil && resp.StatusCode == http.StatusOK {
			resp.Body.Close()
			return NewRemoteEngine(nodeAddr), nil
		}
		if resp != nil {
			resp.Body.Close()
		}
	}

	// Fallback to local LSMEngine
	var lsme *lsmengine.LSMEngine
	var err error
	if walSyncMs > 0 {
		lsme, err = lsmengine.NewLSMEngineWithConfig(cfg.DataDir, lsmengine.LSMConfig{
			WALSyncInterval: time.Duration(walSyncMs) * time.Millisecond,
		})
	} else {
		lsme, err = lsmengine.NewLSMEngine(cfg.DataDir)
	}

	if err != nil {
		return nil, err
	}

	return lsme, nil
}

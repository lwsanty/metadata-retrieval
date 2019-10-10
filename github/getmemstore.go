package github

import (
	"context"
	"fmt"
	"net/http"

	"github.com/src-d/metadata-retrieval/github/store"
)

func GetMemStore(ctx context.Context, client *http.Client, owner, name string) (*store.Mem, error) {
	d, err := NewMemDownloader(client)
	if err != nil {
		return nil, err
	}
	if err := d.DownloadRepository(ctx, owner, name, 0); err != nil {
		return nil, err
	}
	memStore, ok := d.storer.(*store.Mem)
	if !ok {
		return nil, fmt.Errorf("could not type assert to memstore")
	}

	return memStore, nil
}

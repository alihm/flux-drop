package storagepool

import (
	"errors"
	"net/http"
	"strings"

	"github.com/runonflux/flux-drop/internal/metadata"
	"github.com/runonflux/flux-drop/internal/project"
)

func (p *Pool) AdminHandlerWithOperations(store *metadata.Store) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !p.adminAuthorized(r) {
			http.NotFound(w, r)
			return
		}
		if r.URL.Path == "/api/storage/apps" {
			allocations := map[string]project.StorageAllocation{}
			err := store.Run(r.Context(), func(tx *metadata.Tx) error {
				for _, a := range p.apps {
					var value project.StorageAllocation
					err := tx.Get("storage_allocations/"+a.config.AppName, &value)
					if err != nil && !errors.Is(err, metadata.ErrNotFound) {
						return err
					}
					allocations[a.config.AppName] = value
				}
				return nil
			})
			jsonReply(w, 200, map[string]any{"apps": p.appStatuses(), "allocations": allocations, "serving": p.ServingMetrics(), "partial": err != nil, "metadataAvailable": err == nil})
			return
		}
		id, ok := strings.CutPrefix(r.URL.Path, "/api/storage/operations/")
		if !ok || !digestRE.MatchString(id) {
			http.NotFound(w, r)
			return
		}
		var operation project.Operation
		err := store.Run(r.Context(), func(tx *metadata.Tx) error { return tx.Get("operations/"+id, &operation) })
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			storageError(w, err)
			return
		}
		var pr project.Project
		err = store.Run(r.Context(), func(tx *metadata.Tx) error { return tx.Get("projects/"+operation.ProjectID, &pr) })
		if err != nil {
			if errors.Is(err, metadata.ErrNotFound) {
				http.NotFound(w, r)
				return
			}
			storageError(w, err)
			return
		}
		jsonReply(w, 200, map[string]any{"operationId": operation.ID, "projectId": operation.ProjectID, "digest": operation.Digest, "state": operation.State, "expiresAt": operation.ExpiresAt, "storageApp": pr.StorageApp})
	})
}

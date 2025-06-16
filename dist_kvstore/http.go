package dist_kvstore

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

func HttpHandle(ds Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// expect path to be "/kvstore/<key>"
		key, ok := strings.CutPrefix(r.URL.Path, "/kvstore/")
		if !ok {
			http.Error(w, "invalid path, expected `/kvstore/<key>`", http.StatusBadRequest)
			return
		}
		if len(key) == 0 {
			keys := ds.Keys()
			b, err := json.Marshal(keys)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			_, err = w.Write(b)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		switch r.Method {
		case http.MethodGet:
			val, ok := ds.Get(key)
			if !ok {
				http.Error(w, "key not found", http.StatusNotFound)
				return
			}
			_, err := w.Write([]byte(val))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		case http.MethodPost, http.MethodPut:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			ds.Set(key, string(body))
			w.WriteHeader(http.StatusOK)
		case http.MethodDelete:
			ds.Set(key, "")
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method must be GET POST PUT DELETE", http.StatusBadRequest)
			return
		}
	}
}

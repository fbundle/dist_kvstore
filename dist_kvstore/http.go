package dist_kvstore

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
)

func HttpHandle(ds Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println(r.URL.Path)
		if !strings.HasPrefix(r.URL.Path, "/kvstore/") {
			http.NotFound(w, r)
			return
		}
		defer r.Body.Close()

		key, _ := strings.CutPrefix(r.URL.Path, "/kvstore/")
		if len(key) == 0 {
			b, err := json.Marshal(ds.Keys())
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			_, err = w.Write(b)
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
			val := string(body)
			ds.Set(key, val)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method must be GET POST PUT", http.StatusBadRequest)
		}

	}
}

package dist_kvstore

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
)

func HttpHandle(ds Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/kvstore_keys") {
			keys := ds.Keys()
			b, err := json.Marshal(keys)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(b)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
		} else if strings.HasPrefix(r.URL.Path, "/kvstore_next") {
			token := ds.Next()
			tokenStr := strconv.Itoa(token)
			_, err := w.Write([]byte(tokenStr))
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
		} else if strings.HasPrefix(r.URL.Path, "/kvstore/") {
			key, _ := strings.CutPrefix(r.URL.Path, "/kvstore/")
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
				tokenStr := r.URL.Query().Get("token")
				if tokenStr == "" {
					http.Error(w, "token is required", http.StatusBadRequest)
					return
				}
				token, err := strconv.Atoi(tokenStr)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				body, err := io.ReadAll(r.Body)
				if err != nil {
					http.Error(w, err.Error(), http.StatusBadRequest)
					return
				}
				val := string(body)
				ok := ds.Set(token, key, val)
				if !ok {
					http.Error(w, "token conflict", http.StatusConflict)
					return
				}
				w.WriteHeader(http.StatusOK)
			default:
				http.Error(w, "method must be GET POST PUT", http.StatusBadRequest)
			}
		} else {
			http.NotFound(w, r)
		}
	}
}

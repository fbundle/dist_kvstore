package dist_kvstore

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

type versionedValue struct {
	Val string `json:"val"`
	Ver uint64 `json:"ver"`
}

func HttpHandle(ds Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		switch r.Method {
		case http.MethodGet:
			entry := ds.Get(key)
			b, err := json.Marshal(entry)
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			_, err = w.Write(b)
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
			v := versionedValue{}
			err = json.Unmarshal(body, &v)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			cmd := makeCmd([]Entry{
				{
					Key: key,
					Val: v.Val,
					Ver: v.Ver,
				},
			})

			ds.Set(cmd)
			w.WriteHeader(http.StatusOK)
		default:
			http.Error(w, "method must be GET POST PUT", http.StatusBadRequest)
		}

	}
}

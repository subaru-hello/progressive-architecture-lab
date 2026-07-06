// Node 版 app/src/server.ts の /work と「同一の計算」を行う Go サーバー。
// net/http は各リクエストを goroutine で処理し、GOMAXPROCS 個の OS スレッドに分散する
// = CPU 律速の処理が本当に並列で走る（Node の単一 JS スレッドとの対比）。
package main

import (
	"encoding/json"
	"math"
	"net/http"
	"os"
	"strconv"
	"time"
)

func main() {
	hostname, _ := os.Hostname()

	http.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		cpu, _ := strconv.Atoi(r.URL.Query().Get("cpu"))
		if ms > 0 {
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		var sum float64
		n := cpu * 1_000_000 // Node 側と完全に同じ: cpu*1e6 回の sqrt 加算
		for i := 0; i < n; i++ {
			sum += math.Sqrt(float64(i))
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"instance": hostname, "ms": ms, "cpu": cpu, "sum": sum,
		})
	})

	http.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"status":"ok","instance":"` + hostname + `"}`))
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "3001"
	}
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		panic(err)
	}
}

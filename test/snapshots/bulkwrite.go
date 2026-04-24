package main

import (
    "bytes"
    "fmt"
    "net/http"
    "sync"
)

func main() {
    var wg sync.WaitGroup
    sem := make(chan struct{}, 20) // 20 concurrent
    for i := 1; i <= 2000; i++ {
        wg.Add(1)
        sem <- struct{}{}
        go func(i int) {
            defer wg.Done()
            defer func() { <-sem }()
            body := fmt.Sprintf(`{"value":"v%d"}`, i)
            req, _ := http.NewRequest("PUT", 
                fmt.Sprintf("http://localhost:8180/kv/key%d", i),
                bytes.NewBufferString(body))
            req.Header.Set("Content-Type", "application/json")
            http.DefaultClient.Do(req)
        }(i)
    }
    wg.Wait()
    fmt.Println("done")
}
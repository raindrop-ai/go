package main

import (
	"log"
	"net/http"
	"os"
	"strings"

	raindrop "github.com/raindrop-ai/go"
)

// This example starts an HTTP server that receives OTLP/HTTP JSON traces
// (e.g. from OpenRouter's broadcast feature) and forwards them to Raindrop.
//
// Usage:
//   RAINDROP_WRITE_KEY=rk_... go run .
//
// Then configure OpenRouter's OTLP broadcast endpoint to:
//   http://your-server:8090/v1/traces

func main() {
	writeKey := strings.TrimSpace(os.Getenv("RAINDROP_WRITE_KEY"))
	if writeKey == "" {
		log.Fatal("RAINDROP_WRITE_KEY is required")
	}

	client, err := raindrop.New(
		raindrop.WithWriteKey(writeKey),
		raindrop.WithServiceName("my-go-proxy"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	mux := http.NewServeMux()
	mux.Handle("/v1/traces", client.OTLPHandler())

	addr := envOrDefault("ADDR", ":8090")
	log.Printf("OTLP receiver listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func envOrDefault(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

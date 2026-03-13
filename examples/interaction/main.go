package main

import (
	"context"
	"log"

	raindrop "github.com/raindrop-ai/go"
)

func main() {
	client, err := raindrop.New(raindrop.WithWriteKey("rk_..."))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	interaction := client.Begin(context.Background(), raindrop.BeginOptions{
		EventID: "evt_123",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Hello!",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
	})

	if err := interaction.SetProperties(map[string]any{"stage": "processing"}); err != nil {
		log.Fatal(err)
	}

	if err := interaction.Finish(raindrop.FinishOptions{Output: "Hi there!"}); err != nil {
		log.Fatal(err)
	}
}

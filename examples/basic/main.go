package main

import (
	"context"
	"log"

	raindrop "github.com/raindrop-ai/go"
)

func main() {
	client, err := raindrop.New(
		raindrop.WithWriteKey("rk_..."),
		raindrop.WithEndpoint("https://api.raindrop.ai/v1/"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()

	if err := client.Identify(ctx, raindrop.User{
		UserID: "user-123",
		Traits: map[string]any{"plan": "paid"},
	}); err != nil {
		log.Fatal(err)
	}

	if err := client.TrackAI(ctx, raindrop.AIEvent{
		EventID: "evt_123",
		UserID:  "user-123",
		Event:   "ai_generation",
		Input:   "What is the capital of France?",
		Output:  "Paris",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
	}); err != nil {
		log.Fatal(err)
	}

	if err := client.TrackSignal(ctx, raindrop.Signal{
		EventID:   "evt_123",
		Name:      "thumbs_up",
		Type:      "feedback",
		Sentiment: "POSITIVE",
	}); err != nil {
		log.Fatal(err)
	}

	span := client.StartSpan(ctx, raindrop.SpanOptions{
		Name:    "llm_call",
		EventID: "evt_123",
	})
	defer span.End()
	span.SetAttributes(raindrop.StringAttr("ai.model.id", "gpt-4o"))
}

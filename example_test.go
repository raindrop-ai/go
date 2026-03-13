package raindrop_test

import (
	"context"

	raindrop "github.com/invisible-tools/go-raindrop"
)

func ExampleClient_TrackAI() {
	client, _ := raindrop.New(
		raindrop.WithWriteKey("rk_example"),
		raindrop.WithEndpoint("https://api.raindrop.ai/v1/"),
	)
	defer client.Close()

	_ = client.TrackAI(context.Background(), raindrop.AIEvent{
		UserID:  "user-123",
		Event:   "ai_generation",
		Input:   "What is the capital of France?",
		Output:  "Paris",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
		Properties: map[string]any{
			"ai.usage.prompt_tokens":     10,
			"ai.usage.completion_tokens": 5,
		},
	})
}

func ExampleClient_TrackEvent() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	_ = client.TrackEvent(context.Background(), raindrop.Event{
		UserID: "user-123",
		Event:  "session_started",
		Properties: map[string]any{
			"entrypoint": "dashboard",
		},
	})
}

func ExampleClient_Begin() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	interaction := client.Begin(context.Background(), raindrop.BeginOptions{
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Hello!",
		Model:   "gpt-4o",
		ConvoID: "conv-123",
	})

	_ = interaction.SetProperty("stage", "processing")
	_ = interaction.AddAttachments([]raindrop.Attachment{{
		Type:  "text",
		Role:  "output",
		Name:  "reasoning-summary",
		Value: "The user wants a simple friendly plan.",
	}})
	_ = interaction.Finish(raindrop.FinishOptions{Output: "Hi there!"})
}

func ExampleClient_ResumeInteraction() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	_ = client.Begin(context.Background(), raindrop.BeginOptions{
		EventID: "evt_123",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Can you suggest a morning plan?",
	})

	interaction := client.ResumeInteraction("evt_123")
	_ = interaction.SetInput("Can you refine the earlier plan?")
	_ = interaction.SetProperty("stage", "follow-up")
	_ = interaction.Finish(raindrop.FinishOptions{Output: "Here is a tighter version of the plan."})
}

func ExampleWithTool() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	interaction := client.Begin(context.Background(), raindrop.BeginOptions{
		EventID: "evt_123",
		UserID:  "user-123",
		Event:   "chat_message",
		Input:   "Can you find a coffee spot?",
	})

	_, _ = raindrop.WithTool(interaction, "coffee_search", raindrop.ToolOptions{
		Input: map[string]any{"query": "best coffee near Dolores Park"},
	}, func() (map[string]any, error) {
		return map[string]any{"winner": "Ritual Coffee Roasters"}, nil
	})

	_ = interaction.Finish(raindrop.FinishOptions{Output: "Try Ritual Coffee Roasters."})
}

func ExampleClient_Tracer() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	tracer := client.Tracer(map[string]any{"job_id": "batch-123"})
	_ = tracer.WithSpan(raindrop.SpanOptions{
		Name:       "offline_enrichment",
		Properties: map[string]any{"step": "embed"},
	}, func(ctx context.Context, span *raindrop.Span) error {
		span.SetAttributes(raindrop.StringAttr("job.kind", "offline"))
		_ = ctx
		return nil
	})

	tracer.TrackTool(raindrop.TrackToolOptions{
		Name:       "vector_lookup",
		Input:      map[string]any{"query": "best coffee"},
		Output:     map[string]any{"winner": "Ritual Coffee Roasters"},
		Properties: map[string]any{"step": "retrieve"},
	})
}

func ExampleClient_TrackSignal() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	_ = client.TrackSignal(context.Background(), raindrop.Signal{
		EventID:   "evt_123",
		Name:      "thumbs_up",
		Type:      "feedback",
		Sentiment: "POSITIVE",
	})
}

func ExampleClient_Identify() {
	client, _ := raindrop.New(raindrop.WithWriteKey("rk_example"))
	defer client.Close()

	_ = client.Identify(context.Background(), raindrop.User{
		UserID: "user-123",
		Traits: map[string]any{"plan": "paid"},
	})
}

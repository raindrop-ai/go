package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	raindrop "github.com/invisible-tools/go-raindrop"
)

func main() {
	writeKey := os.Getenv("RAINDROP_WRITE_KEY")
	if writeKey == "" {
		log.Fatal("RAINDROP_WRITE_KEY is required")
	}

	runID := fmt.Sprintf("go-sdk-smoke-%d", time.Now().UTC().UnixNano())
	userID := "smoke-user-" + runID
	convoID := "smoke-convo-" + runID
	oneShotEventID := "smoke-oneshot-" + runID
	interactionEventID := "smoke-interaction-" + runID

	client, err := raindrop.New(
		raindrop.WithWriteKey(writeKey),
		raindrop.WithDebug(true),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		if err := client.Close(); err != nil {
			log.Printf("close error: %v", err)
		}
	}()

	ctx := context.Background()

	if err := client.Identify(ctx, raindrop.User{
		UserID: userID,
		Traits: map[string]any{
			"plan":        "sdk-smoke",
			"test_run_id": runID,
			"source":      "raindrop-go",
		},
	}); err != nil {
		log.Fatal(err)
	}

	if err := client.TrackAI(ctx, raindrop.AIEvent{
		EventID: oneShotEventID,
		UserID:  userID,
		Event:   "chat_message",
		Input:   "Hey there, can you help me plan a calm Saturday morning with coffee and a short walk? " + runID,
		Output:  "Absolutely. Start with coffee at 9:00 AM, take a 20-minute walk around 9:45, and keep the rest of the morning open for something low-key like reading or brunch. " + runID,
		Model:   "gpt-4o",
		ConvoID: convoID,
		Properties: map[string]any{
			"test_run_id":                  runID,
			"smoke.kind":                   "friendly_oneshot",
			"ai.usage.prompt_tokens":       11,
			"ai.usage.completion_tokens":   7,
			"ai.usage.total_tokens":        18,
			"smoke.dashboard_lookup_value": runID,
			"surface":                      "chat",
		},
	}); err != nil {
		log.Fatal(err)
	}

	interaction := client.Begin(ctx, raindrop.BeginOptions{
		EventID: interactionEventID,
		UserID:  userID,
		Event:   "chat_message",
		Input:   "Hi! I'm visiting San Francisco this weekend. Can you suggest a friendly morning plan and check whether Dolores Park is a good outdoor stop? " + runID,
		Model:   "gpt-4o",
		ConvoID: convoID,
		Properties: map[string]any{
			"test_run_id": runID,
			"smoke.kind":  "friendly_interaction",
			"surface":     "chat",
		},
	})

	if err := interaction.SetProperties(map[string]any{
		"stage":       "processing",
		"test_run_id": runID,
	}); err != nil {
		log.Fatal(err)
	}
	if err := interaction.AddAttachments([]raindrop.Attachment{{
		Type:  "text",
		Role:  "output",
		Name:  "reasoning-summary",
		Value: "User wants a relaxed San Francisco morning plan with coffee, a park stop, and a short walk.",
	}}); err != nil {
		log.Fatal(err)
	}

	rootSpan := client.StartSpan(ctx, raindrop.SpanOptions{
		Name:    "conversation_turn",
		EventID: interactionEventID,
	})
	rootSpan.SetAttributes(
		raindrop.StringAttr("ai.model.id", "gpt-4o"),
		raindrop.StringAttr("test.run_id", runID),
		raindrop.IntAttr("ai.usage.prompt_tokens", 13),
		raindrop.StringAttr("conversation.surface", "chat"),
		raindrop.StringAttr("conversation.intent", "friendly_trip_planning"),
	)
	sleep(120 * time.Millisecond)

	intentSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "intent_analysis",
		Parent: rootSpan,
		Properties: map[string]any{
			"analysis.goal":        "plan_weekend_morning",
			"analysis.destination": "san_francisco",
			"analysis.tone":        "friendly",
		},
	})
	sleep(180 * time.Millisecond)
	intentSpan.End()
	sleep(60 * time.Millisecond)

	contextSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "context_building",
		Parent: rootSpan,
		Properties: map[string]any{
			"context.preferences": "coffee, walk, relaxed pace",
			"context.timeframe":   "weekend morning",
		},
	})
	sleep(90 * time.Millisecond)

	weatherToolSpan := interaction.StartToolSpan("weather_lookup", raindrop.ToolOptions{
		Parent: contextSpan,
		Input: map[string]any{
			"location": "San Francisco",
			"question": "Is Dolores Park a good stop on Saturday morning?",
		},
		Properties: map[string]any{
			"user_id":     userID,
			"convo_id":    convoID,
			"test_run_id": runID,
		},
	})
	sleep(240 * time.Millisecond)
	weatherToolSpan.SetOutput(map[string]any{
		"forecast":            "Cool and sunny",
		"park_recommendation": "Yes",
	})
	weatherToolSpan.End()
	sleep(80 * time.Millisecond)

	parkCheckToolSpan := interaction.StartToolSpan("park_check", raindrop.ToolOptions{
		Parent: contextSpan,
		Input: map[string]any{
			"location": "Dolores Park",
			"time":     "Saturday morning",
		},
		Properties: map[string]any{
			"user_id":     userID,
			"convo_id":    convoID,
			"test_run_id": runID,
		},
	})
	sleep(170 * time.Millisecond)
	parkCheckToolSpan.SetOutput(map[string]any{
		"crowds":         "light",
		"vibe":           "quiet",
		"recommendation": "strong_yes",
	})
	parkCheckToolSpan.End()
	sleep(70 * time.Millisecond)

	coffeeToolSpan := interaction.StartToolSpan("coffee_search", raindrop.ToolOptions{
		Parent: contextSpan,
		Input: map[string]any{
			"neighborhood": "Mission District",
			"criteria":     []string{"good coffee", "short walk", "morning-friendly"},
		},
		Properties: map[string]any{
			"user_id":     userID,
			"convo_id":    convoID,
			"test_run_id": runID,
		},
	})
	sleep(210 * time.Millisecond)
	coffeeToolSpan.SetOutput(map[string]any{
		"candidates": []string{"Ritual Coffee Roasters", "Four Barrel Coffee", "Stable Cafe"},
	})
	coffeeToolSpan.End()
	sleep(75 * time.Millisecond)

	walkPlannerToolSpan := interaction.StartToolSpan("neighborhood_walk_planner", raindrop.ToolOptions{
		Parent: contextSpan,
		Input: map[string]any{
			"start":            "Dolores Park",
			"duration_minutes": 25,
		},
		Properties: map[string]any{
			"user_id":     userID,
			"convo_id":    convoID,
			"test_run_id": runID,
		},
	})
	sleep(260 * time.Millisecond)
	walkPlannerToolSpan.SetOutput(map[string]any{
		"route": "Dolores Park -> Valencia coffee stop -> back through the Mission",
		"pace":  "easy",
	})
	walkPlannerToolSpan.End()
	sleep(110 * time.Millisecond)
	contextSpan.End()
	sleep(90 * time.Millisecond)

	planSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "plan_synthesis",
		Parent: rootSpan,
		Properties: map[string]any{
			"plan.style":           "calm_local_itinerary",
			"plan.candidate_count": 4,
		},
	})
	sleep(140 * time.Millisecond)

	optionRankingSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "option_ranking",
		Parent: planSpan,
		Properties: map[string]any{
			"ranking.criteria":   "walkability, atmosphere, simplicity",
			"ranking.top_choice": "coffee_then_dolores_park",
		},
	})
	sleep(190 * time.Millisecond)
	optionRankingSpan.End()
	sleep(65 * time.Millisecond)

	safetyCheckSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "response_safety_check",
		Parent: planSpan,
		Properties: map[string]any{
			"safety.scope":  "travel_recommendation",
			"safety.result": "pass",
		},
	})
	sleep(160 * time.Millisecond)
	safetyCheckSpan.End()
	sleep(60 * time.Millisecond)
	planSpan.End()
	sleep(100 * time.Millisecond)

	finalWriteSpan := interaction.StartSpan(raindrop.SpanOptions{
		Name:   "final_response_write",
		Parent: rootSpan,
		Properties: map[string]any{
			"response.style":      "warm_and_concise",
			"response.tool_count": 4,
		},
	})
	sleep(220 * time.Millisecond)
	finalWriteSpan.End()
	sleep(120 * time.Millisecond)

	if err := interaction.Finish(raindrop.FinishOptions{
		Output: "Definitely. A nice Saturday morning would be coffee near Valencia around 9:00 AM, then a relaxed walk through Dolores Park while it's still calm and sunny. After that, you could grab brunch nearby and keep the rest of the day flexible. " + runID,
		Properties: map[string]any{
			"test_run_id":                runID,
			"stage":                      "complete",
			"ai.usage.prompt_tokens":     13,
			"ai.usage.completion_tokens": 9,
			"tool_count":                 4,
			"trace_span_count_hint":      11,
			"surface":                    "chat",
		},
	}); err != nil {
		log.Fatal(err)
	}
	rootSpan.End()

	if err := client.TrackSignal(ctx, raindrop.Signal{
		EventID:   interactionEventID,
		Name:      "thumbs_up",
		Type:      "feedback",
		Sentiment: "POSITIVE",
		Properties: map[string]any{
			"test_run_id": runID,
			"source":      "go-sdk-smoke",
		},
	}); err != nil {
		log.Fatal(err)
	}

	if err := client.Flush(ctx); err != nil {
		log.Fatal(err)
	}

	fmt.Printf("run_id=%s\n", runID)
	fmt.Printf("user_id=%s\n", userID)
	fmt.Printf("convo_id=%s\n", convoID)
	fmt.Printf("oneshot_event_id=%s\n", oneShotEventID)
	fmt.Printf("interaction_event_id=%s\n", interactionEventID)
	fmt.Println("dashboard_lookup_hint=test_run_id")
}

func sleep(d time.Duration) {
	time.Sleep(d)
}

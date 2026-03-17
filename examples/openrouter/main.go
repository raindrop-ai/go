package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	raindrop "github.com/raindrop-ai/go"
	otelattribute "go.opentelemetry.io/otel/attribute"
	otelcodes "go.opentelemetry.io/otel/codes"
	sdkresource "go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	oteltrace "go.opentelemetry.io/otel/trace"
)

const openRouterEndpoint = "https://openrouter.ai/api/v1/chat/completions"

type openRouterMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type openRouterChatRequest struct {
	Model     string              `json:"model"`
	Messages  []openRouterMessage `json:"messages"`
	User      string              `json:"user,omitempty"`
	SessionID string              `json:"session_id,omitempty"`
	MaxTokens int                 `json:"max_tokens,omitempty"`
}

type openRouterChatResponse struct {
	ID      string                 `json:"id"`
	Model   string                 `json:"model"`
	Choices []openRouterChatChoice `json:"choices"`
	Usage   *openRouterUsage       `json:"usage"`
}

type openRouterChatChoice struct {
	Message openRouterMessage `json:"message"`
}

type openRouterUsage struct {
	PromptTokens     int `json:"prompt_tokens"`
	CompletionTokens int `json:"completion_tokens"`
	TotalTokens      int `json:"total_tokens"`
}

type weatherResult struct {
	Forecast     string `json:"forecast"`
	TemperatureF int    `json:"temperature_f"`
}

func main() {
	writeKey := strings.TrimSpace(os.Getenv("RAINDROP_WRITE_KEY"))
	if writeKey == "" {
		log.Fatal("RAINDROP_WRITE_KEY is required")
	}

	openRouterKey := strings.TrimSpace(os.Getenv("OPENROUTER_API_KEY"))
	if openRouterKey == "" {
		log.Fatal("OPENROUTER_API_KEY is required")
	}

	model := envOrDefault("OPENROUTER_MODEL", "openai/gpt-4o-mini")
	userID := "user-123"
	convoID := "conv-123"
	prompt := "Plan a calm Saturday morning in San Francisco."

	client, err := raindrop.New(
		raindrop.WithWriteKey(writeKey),
		raindrop.WithEndpoint("https://api.raindrop.ai/v1/"),
	)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	ctx := context.Background()
	httpClient := &http.Client{Timeout: 30 * time.Second}
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(client.OTelSpanExporter()),
		sdktrace.WithResource(sdkresource.NewSchemaless(
			otelattribute.String("service.name", "openrouter-go-example"),
			otelattribute.String("service.version", raindrop.Version),
		)),
	)
	defer func() { _ = tp.Shutdown(ctx) }()

	tracer := tp.Tracer("github.com/raindrop-ai/go/examples/openrouter")
	interaction := client.Begin(ctx, raindrop.BeginOptions{
		UserID:  userID,
		Event:   "chat_message",
		Input:   prompt,
		ConvoID: convoID,
		Properties: map[string]any{
			"llm.provider": "openrouter",
		},
	})

	_, weatherSpan := tracer.Start(ctx, "weather_lookup",
		oteltrace.WithAttributes(raindrop.OTelToolAttributes("weather_lookup", map[string]any{"location": "San Francisco"}, nil, map[string]any{
			"event_id": interaction.EventID(),
		})...),
	)
	weather, err := lookupWeather("San Francisco")
	if err != nil {
		weatherSpan.RecordError(err)
		weatherSpan.SetStatus(otelcodes.Error, err.Error())
		weatherSpan.End()
		log.Fatal(err)
	}
	weatherSpan.SetAttributes(raindrop.OTelToolAttributes("weather_lookup", nil, weather, nil)...)
	weatherSpan.End()

	llmCtx, llmSpan := tracer.Start(ctx, "openrouter.chat.completion",
		oteltrace.WithSpanKind(oteltrace.SpanKindClient),
		oteltrace.WithAttributes(
			otelattribute.String("ai.telemetry.metadata.raindrop.eventId", interaction.EventID()),
			otelattribute.String("ai.model.provider", "openrouter"),
			otelattribute.String("gen_ai.request.model", model),
		),
	)
	completion, err := createChatCompletion(llmCtx, httpClient, openRouterKey, model, userID, convoID, prompt, weather)
	if err != nil {
		llmSpan.RecordError(err)
		llmSpan.SetStatus(otelcodes.Error, err.Error())
		llmSpan.End()
		log.Fatal(err)
	}
	if completion.Usage != nil {
		llmSpan.SetAttributes(
			otelattribute.String("gen_ai.response.model", completion.Model),
			otelattribute.Int("gen_ai.usage.prompt_tokens", completion.Usage.PromptTokens),
			otelattribute.Int("gen_ai.usage.completion_tokens", completion.Usage.CompletionTokens),
		)
	}
	llmSpan.End()

	reply, err := assistantText(completion)
	if err != nil {
		log.Fatal(err)
	}

	properties := map[string]any{
		"ai.model.provider": "openrouter",
	}
	if completion.Usage != nil {
		properties["gen_ai.usage.prompt_tokens"] = completion.Usage.PromptTokens
		properties["gen_ai.usage.completion_tokens"] = completion.Usage.CompletionTokens
	}

	if err := interaction.Finish(raindrop.FinishOptions{
		Output:     reply,
		Model:      completion.Model,
		Properties: properties,
	}); err != nil {
		log.Fatal(err)
	}

	fmt.Println(reply)
}

func createChatCompletion(ctx context.Context, httpClient *http.Client, apiKey, model, userID, convoID, prompt string, weather weatherResult) (openRouterChatResponse, error) {
	reqBody := openRouterChatRequest{
		Model: model,
		Messages: []openRouterMessage{
			{
				Role:    "system",
				Content: fmt.Sprintf("You are a helpful local planner. Weather context: %s, %dF.", weather.Forecast, weather.TemperatureF),
			},
			{
				Role:    "user",
				Content: prompt,
			},
		},
		User:      userID,
		SessionID: convoID,
		MaxTokens: 300,
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return openRouterChatResponse{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, openRouterEndpoint, bytes.NewReader(body))
	if err != nil {
		return openRouterChatResponse{}, err
	}
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Content-Type", "application/json")
	if referer := strings.TrimSpace(os.Getenv("OPENROUTER_HTTP_REFERER")); referer != "" {
		req.Header.Set("HTTP-Referer", referer)
	}
	if title := strings.TrimSpace(os.Getenv("OPENROUTER_TITLE")); title != "" {
		req.Header.Set("X-Title", title)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		return openRouterChatResponse{}, err
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return openRouterChatResponse{}, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return openRouterChatResponse{}, fmt.Errorf("openrouter chat completion failed: %s: %s", resp.Status, strings.TrimSpace(string(respBody)))
	}

	var decoded openRouterChatResponse
	if err := json.Unmarshal(respBody, &decoded); err != nil {
		return openRouterChatResponse{}, err
	}
	return decoded, nil
}

func assistantText(response openRouterChatResponse) (string, error) {
	if len(response.Choices) == 0 {
		return "", errors.New("openrouter returned no choices")
	}

	text := strings.TrimSpace(response.Choices[0].Message.Content)
	if text == "" {
		return "", errors.New("openrouter returned an empty assistant message")
	}
	return text, nil
}

func lookupWeather(location string) (weatherResult, error) {
	switch location {
	case "San Francisco":
		return weatherResult{
			Forecast:     "sunny",
			TemperatureF: 65,
		}, nil
	default:
		return weatherResult{
			Forecast:     "mild",
			TemperatureF: 68,
		}, nil
	}
}

func envOrDefault(key, fallback string) string {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		return value
	}
	return fallback
}

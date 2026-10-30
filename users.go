package raindrop

import "context"

type User struct {
	UserID string
	Traits map[string]any
}

type identifyPayload struct {
	UserID string         `json:"user_id"`
	Traits map[string]any `json:"traits"`
}

func (c *Client) Identify(ctx context.Context, user User) error {
	if c == nil || !c.enabled {
		return nil
	}
	if err := c.ensureOpen(); err != nil {
		return err
	}

	payload := identifyPayload{
		UserID: user.UserID,
		Traits: cloneMap(user.Traits),
	}
	if payload.Traits == nil {
		payload.Traits = map[string]any{}
	}
	// Queued, never sent on the caller (CONTRACT 1.1).
	c.track.Enqueue(routeUsersIdentify, payload)
	return nil
}

package models

import (
	"time"
)

// CEFLogEntry represents a parsed CEF log entry
type CEFLogEntry struct {
	DeviceVendor  string    `json:"device_vendor"`
	DeviceProduct string    `json:"device_product"`
	EventID       string    `json:"event_id"`
	Severity      int       `int:"severity"`
	Src           string    `json:"src"`
	Dst           string    `json:"dst"`
	Name          string    `json:"name"`
	Timestamp     time.Time `json:"timestamp"`
	RawLog        string    `json:"raw_log"`
}

// AITool represents the structure of the AI tools table in Supabase
type AITool struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	URL         string `json:"url"`
	Content     string `json:"content"`
	HasAI       bool   `json:"has_ai"`
	AICategory  string `json:"ai_category"`
	RedirectURL string `json:"redirect_url"`
	IconURL     string `json:"icon_url"`
}

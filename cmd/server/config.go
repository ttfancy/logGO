package main

import (
	"fmt"
	"os"
)

// config is read from the environment — CONTOGETHER_URL and
// CONTOGETHER_API_KEY are required (there's nothing sensible to
// default an integration target to); everything else has a local-dev
// default.
type config struct {
	ContogetherURL    string
	ContogetherAPIKey string
	SourceName        string
	Port              string
	LogFilePath       string
}

func loadConfig() (*config, error) {
	url := os.Getenv("CONTOGETHER_URL")
	if url == "" {
		return nil, fmt.Errorf("CONTOGETHER_URL is required, e.g. http://localhost:8080")
	}
	apiKey := os.Getenv("CONTOGETHER_API_KEY")
	if apiKey == "" {
		return nil, fmt.Errorf("CONTOGETHER_API_KEY is required")
	}

	return &config{
		ContogetherURL:    url,
		ContogetherAPIKey: apiKey,
		SourceName:        stringEnv("SOURCE_NAME", "conTogether"),
		Port:              stringEnv("PORT", "9090"),
		LogFilePath:       stringEnv("LOG_FILE_PATH", "logGO.log"),
	}, nil
}

func stringEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

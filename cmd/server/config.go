package main

import "os"

// config is read from the environment — everything here now has a
// local-dev default, including what used to be the only way to point
// logGO at a conTogether instance (CONTOGETHER_URL/CONTOGETHER_API_KEY):
// that's an optional convenience that auto-registers as an ordinary
// source at boot now (see main.go), not the only way in — the primary
// path is POST /sources (and the UI's "Add service" button) at runtime.
type config struct {
	ContogetherURL    string
	ContogetherAPIKey string
	SourceName        string
	Port              string
	LogFilePath       string
	SourcesFilePath   string
}

func loadConfig() *config {
	return &config{
		ContogetherURL:    os.Getenv("CONTOGETHER_URL"),
		ContogetherAPIKey: os.Getenv("CONTOGETHER_API_KEY"),
		SourceName:        stringEnv("SOURCE_NAME", "conTogether"),
		Port:              stringEnv("PORT", "9090"),
		LogFilePath:       stringEnv("LOG_FILE_PATH", "logGO.log"),
		SourcesFilePath:   stringEnv("SOURCES_FILE", "sources.json"),
	}
}

func stringEnv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

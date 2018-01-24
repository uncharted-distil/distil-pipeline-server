package env

import (
	"strings"

	"github.com/caarlos0/env"
)

var (
	cfg *Config
)

// Config represents the application configuration state loaded from env vars.
type Config struct {
	ResultDir  string  `env:"PIPELINE_SERVER_RESULT_DIR" envDefault:"./results"`
	Port       string  `env:"PIPELINE_SERVER_PORT" envDefault:":45042"`
	SendDelay  int     `env:"PIPELINE_SEND_DELAY" envDefault:"5000"`
	ErrPercent float64 `env:"PIPELINE_ERR_PERCENT" envDefault:"0.1"`
	NumUpdates int     `env:"PIPELINE_NUM_UPDATES" envDefault:"1"`
}

// LoadConfig loads the config from the environment if necessary and returns a
// copy.
func LoadConfig() (Config, error) {
	if cfg == nil {
		cfg = &Config{}
		err := env.Parse(cfg)
		if err != nil {
			return Config{}, err
		}
		// ensure port has ":" prefix
		if !strings.HasPrefix(cfg.Port, ":") {
			cfg.Port = ":" + cfg.Port
		}
	}
	return *cfg, nil
}

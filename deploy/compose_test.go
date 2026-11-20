package deploy

import (
	"bufio"
	"os"
	"strings"
	"testing"
)

// TestComposePassesEveryEnvExampleVariable keeps docker-compose.yml in step with .env.example.
// Compose passes configuration through by interpolation (`VAR: ${VAR:-}`) rather than env_file,
// for Compose v1 compatibility, so a variable missing from the compose file would be silently
// ignored when set in .env.
func TestComposePassesEveryEnvExampleVariable(t *testing.T) {
	f, err := os.Open("../.env.example")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = f.Close() }()

	compose, err := os.ReadFile("../docker-compose.yml")
	if err != nil {
		t.Fatal(err)
	}

	n := 0
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, _, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		n++
		if !strings.Contains(string(compose), key+": ${"+key+":-}") {
			t.Errorf("docker-compose.yml does not pass %s through (want `%s: ${%s:-}`)", key, key, key)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	if n < 20 {
		t.Fatalf("parsed only %d variables from .env.example", n)
	}
}

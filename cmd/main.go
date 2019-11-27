package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/projectriff/http-source/pkg"
)

func main() {

	requireEnvVars("OUTPUTS", "OUTPUT_CONTENT_TYPES")

	outputs := strings.Split(os.Getenv("OUTPUTS"), ",")
	contentTypes := strings.Split(os.Getenv("OUTPUT_CONTENT_TYPES"), ",")
	if len(outputs) != len(contentTypes) {
		panic(fmt.Sprintf("OUTPUTS and OUTPUT_CONTENT_TYPES lists should be of the same size. %d != %d", len(outputs), len(contentTypes)))
	}

	s, err := pkg.NewSource(outputs, contentTypes)
	if err != nil {
		panic(err)
	}

	stopCh := make(<-chan struct{})
	if err := s.Run(stopCh); err != nil {
		panic(err)
	}
}

func requireEnvVars(vars ...string) {
	for _, v := range vars {
		if _, set := os.LookupEnv(v); !set {
			panic(fmt.Sprintf("Required environment variable %q not set", v))
		}
	}
}

package main

import (
	"fmt"
	"os"
	"strings"

	"github.com/projectriff/http-source/pkg"
)

func main() {

	requireEnvVars("OUTPUTS", "CNB_BINDINGS")

	outputs := strings.Split(os.Getenv("OUTPUTS"), ",")

	s, err := pkg.NewSource(os.Getenv("CNB_BINDINGS"), outputs)
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

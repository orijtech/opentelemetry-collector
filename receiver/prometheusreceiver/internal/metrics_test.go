// Copyright The OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal_test

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/receiver/prometheusreceiver"
)

func TestEndToEndUpMetricProduced(t *testing.T) {
	// 1. Create the end point would scrape.
	cst := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, req *http.Request) {
	}))
	defer cst.Close()

	// 2. Create the actual pipeline from YAML.
	u, err := url.Parse(cst.URL)
	require.Nil(t, err)
	yamlConfig := fmt.Sprintf(`
prometheus:
    config:
        scrape_configs:
            - job_name: "e2e"
              scrape_interval: 10us
              static_configs:
              - targets: [%q]
    `, u.Host)
	parser, err := config.NewParserFromBuffer(strings.NewReader(yamlConfig))
	require.Nil(t, err)
	fullConfig := new(prometheusreceiver.Config)
	if err := fullConfig.UnmarshalExtract(parser); err != nil {
		t.Fatalf("Failed to parse config from YAML: %v", err)
	}
	require.Nil(t, fullConfig.Validate())
}

//
// Copyright 2025 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package app

import (
	"flag"
	"log"

	"github.com/spf13/cobra"
	"sigs.k8s.io/release-utils/version"
)

// serveCmd represents the serve command
var serveCmd = &cobra.Command{
	Use:   "serve",
	Short: "start amber server with configured api",
	Long:  `Starts a amber server and serves the configured api`,
	Run: func(_ *cobra.Command, _ []string) {
		// from https://github.com/golang/glog/commit/fca8c8854093a154ff1eb580aae10276ad6b1b5f
		_ = flag.CommandLine.Parse([]string{})

		vi := version.GetVersionInfo()
		viStr, err := vi.JSONString()
		if err != nil {
			viStr = vi.String()
		}
		log.Printf("starting amber-polymerize @ %v", viStr)
	},
}

func init() {
	rootCmd.AddCommand(serveCmd)
}

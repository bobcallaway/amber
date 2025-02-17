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
	"log"
	"net"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "amber-polymerize",
	Short: "utility that scrapes a trillian log and creates entries in a C2SP-compliant GCS bucket backend",
	Long:  "utility that scrapes a trillian log and creates entries in a C2SP-compliant GCS bucket backend",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	rootCmd.PersistentFlags().IP("trillian_log_server.address", net.ParseIP("127.0.0.1"), "Address to connect to")
	rootCmd.PersistentFlags().Uint("trillian_log_server.port", 8080, "Port to connect to")
	rootCmd.PersistentFlags().Int("trillian_log_server.log_id", -1, "ID of tree to read from")
	rootCmd.PersistentFlags().Uint("start", 0, "First entry to copy")
	rootCmd.PersistentFlags().Int("finish", -1, "Last entry to copy; -1 copies all entries from start to end of log")

	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Fatal(err)
	}
}

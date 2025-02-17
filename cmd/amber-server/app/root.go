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
	Use:   "amber-server",
	Short: "amber service that translates Trillan gRPC read calls to C2SP-compliant tile backends",
	Long:  "amber service that translates Trillan gRPC read calls to C2SP-compliant tile backends",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		log.Fatal(err)
	}
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().String("config", "config.yaml", "config file (default is config.yaml)")

	rootCmd.PersistentFlags().IP("address", net.ParseIP("127.0.0.1"), "Address to bind to")
	rootCmd.PersistentFlags().Uint("port", 8080, "Port to bind to")

	if err := viper.BindPFlags(rootCmd.PersistentFlags()); err != nil {
		log.Fatal(err)
	}
}

// initConfig reads in the config file and environment variables
func initConfig() {
	// Set the config file path
	configFile, _ := rootCmd.Flags().GetString("config")
	viper.SetConfigFile(configFile)

	// Read the config file
	if err := viper.ReadInConfig(); err != nil {
		log.Fatal("Error reading config file: ", err)
	}
}

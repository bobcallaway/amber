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

package config

import (
	"encoding/json"
	"log"
	"strconv"
)

type Config struct {
	LogConfigs map[int64]logConfig
}

type logConfig struct {
	FrozenTime int64  `json:"frozenTime"`
	BucketName string `json:"bucketName"`
}

func NewConfig(configMap map[string]any) (*Config, error) {
	config := &Config{LogConfigs: make(map[int64]logConfig, len(configMap))}

	for k, v := range configMap {
		logID, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			return nil, err
		}

		// Marshal each value and unmarshal into logConfig
		b, err := json.Marshal(v)
		if err != nil {
			return nil, err
		}

		var lc logConfig
		if err := json.Unmarshal(b, &lc); err != nil {
			return nil, err
		}

		config.LogConfigs[logID] = lc
	}

	log.Printf("config: %+v\n", config)
	return config, nil
}

//
//   Copyright © 2019 Uncharted Software Inc.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

package pipeline

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

func createRanking(solutionID string, datasetURI string, resultURI string) (string, error) {
	// load the source data
	dataPath := strings.Replace(datasetURI, "file://", "", 1)
	dataPath = strings.Replace(dataPath, "datasetDoc.json", "", 1)
	schema, err := loadDataSchema(dataPath)
	if err != nil {
		log.Warnf("unable to rank data since schema cannot be loaded - %v", err)
		return "", err
	}

	if len(schema.DataResources) != 1 {
		return "", errors.Errorf("expected only 1 data resource but got %d", len(schema.DataResources))
	}
	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output should be header row, and 1 row per variable (name,importance)
	err = writer.Write([]string{"name", "importance"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write header in output")
	}

	for _, v := range schema.DataResources[0].Variables {
		err = writer.Write([]string{fmt.Sprintf("%d", v.ColIndex), fmt.Sprintf("%f", rand.Float64())})
		if err != nil {
			return "", errors.Wrap(err, "unable to write ranking in output")
		}
	}
	resultDir := path.Join(resultURI, fmt.Sprintf("%s-%d", solutionID, 0))
	if err := os.MkdirAll(resultDir, 0777); err != nil && !os.IsExist(err) {
		return "", errors.Wrap(err, "unable to create ranking output directory")
	}

	resultPath := path.Join(resultDir, "results.csv")

	writer.Flush()
	err = ioutil.WriteFile(resultPath, output.Bytes(), 0644)
	if err != nil {
		return "", errors.Wrap(err, "error writing ranking output")
	}

	absResultPath, err := filepath.Abs(resultPath)
	if err != nil {
		return "", errors.Wrap(err, "Failed to generate absolute path")
	}

	return absResultPath, nil
}

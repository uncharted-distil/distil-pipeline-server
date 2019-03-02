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
	fmt "fmt"
	"os"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	log "github.com/unchartedsoftware/plog"
)

func TestCreateClassificationResultsNoSchema(t *testing.T) {
	solutionID := "ABCDEF"
	solutionDir := fmt.Sprintf("%s-0", solutionID)
	resultPath, err := createResults(solutionID, "", "./", "test_feature", TaskType_CLASSIFICATION)
	assert.NoError(t, err)
	assert.NotEmpty(t, resultPath)
	data, err := loadDataCsv(solutionDir)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(data))

	assert.Equal(t, 2, len(data[0]))
	_, err = strconv.Atoi(data[1][0])
	assert.NoError(t, err)
	assert.NotEmpty(t, data[1][1])

	err = os.RemoveAll(solutionDir)
	if err != nil {
		log.Warnf("Failed to clean up test output for %s", solutionID)
	}
}

func TestCreateRegressionResultsNoSchema(t *testing.T) {
	solutionID := "ABCDEF"
	solutionDir := fmt.Sprintf("%s-0", solutionID)
	resultPath, err := createResults(solutionID, "", "./", "test_feature", TaskType_REGRESSION)
	assert.NoError(t, err)
	assert.NotEmpty(t, resultPath)
	data, err := loadDataCsv(solutionDir)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(data))

	assert.Equal(t, 2, len(data[0]))
	_, err = strconv.Atoi(data[1][0])
	assert.NoError(t, err)
	_, err = strconv.ParseFloat(data[1][1], 64)
	assert.NoError(t, err)

	err = os.RemoveAll(solutionDir)
	if err != nil {
		log.Warnf("Failed to clean up test output for %s", solutionID)
	}
}

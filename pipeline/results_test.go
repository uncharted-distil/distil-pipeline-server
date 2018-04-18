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
	pipelineID := "ABCDEF"
	pipelineDir := fmt.Sprintf("%s-0", pipelineID)
	resultPath, err := createResults(pipelineID, "", "./", "test_feature", TaskType_CLASSIFICATION)
	assert.NoError(t, err)
	assert.NotEmpty(t, resultPath)
	data, err := loadDataCsv(pipelineDir)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(data))

	assert.Equal(t, 2, len(data[0]))
	_, err = strconv.Atoi(data[1][0])
	assert.NoError(t, err)
	assert.NotEmpty(t, data[1][1])

	err = os.RemoveAll(pipelineDir)
	if err != nil {
		log.Warnf("Failed to clean up test output for %s", pipelineID)
	}
}

func TestCreateRegressionResultsNoSchema(t *testing.T) {
	pipelineID := "ABCDEF"
	pipelineDir := fmt.Sprintf("%s-0", pipelineID)
	resultPath, err := createResults(pipelineID, "", "./", "test_feature", TaskType_REGRESSION)
	assert.NoError(t, err)
	assert.NotEmpty(t, resultPath)
	data, err := loadDataCsv(pipelineDir)
	assert.NoError(t, err)
	assert.Equal(t, 100, len(data))

	assert.Equal(t, 2, len(data[0]))
	_, err = strconv.Atoi(data[1][0])
	assert.NoError(t, err)
	_, err = strconv.ParseFloat(data[1][1], 64)
	assert.NoError(t, err)

	err = os.RemoveAll(pipelineDir)
	if err != nil {
		log.Warnf("Failed to clean up test output for %s", pipelineID)
	}
}

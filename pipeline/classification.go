package pipeline

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io/ioutil"
	"path"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	log "github.com/unchartedsoftware/plog"
)

func createClassification(solutionID string, datasetURI string, resultURI string) (string, error) {
	// load the source data
	dataPath := strings.Replace(datasetURI, "file://", "", 1)
	dataPath = strings.Replace(dataPath, "datasetDoc.json", "", 1)
	schema, err := loadDataSchema(dataPath)
	if err != nil {
		log.Warnf("unable to classify data since schema cannot be loaded - %v", err)
		return "", err
	}

	if len(schema.DataResources) > 0 {
		return "", errors.Errorf("expected only 1 data resource but got %d", len(schema.DataResources))
	}
	// initialize csv writer
	output := &bytes.Buffer{}
	writer := csv.NewWriter(output)

	// output should be header row, and 1 row per variable (name,labels,probabilities)
	err = writer.Write([]string{"d3mIndex", "labels", "probabilities"})
	if err != nil {
		return "", errors.Wrap(err, "unable to write header in output")
	}

	for _, v := range schema.DataResources[0].Variables {
		err = writer.Write([]string{v.ColName, "['categorical','text']", "[0.875, 0.657]"})
		if err != nil {
			return "", errors.Wrap(err, "unable to write classification in output")
		}
	}
	resultDir := path.Join(resultURI, fmt.Sprintf("%s-%d", solutionID, 0))
	outputDataPath := path.Join(resultDir, "tables", "learningData.csv")
	outputSchemaPath := path.Join(resultDir, "datasetDoc.json")

	writer.Flush()
	err = ioutil.WriteFile(outputDataPath, output.Bytes(), 0644)
	if err != nil {
		return "", errors.Wrap(err, "error writing classification output")
	}

	schemaOutput := `{
  "about": {
    "datasetID": "%s_classification",
    "datasetName": "NULL",
    "datasetSchemaVersion": "3.0"
  },
  "dataResources": [
    {
      "resID": "0",
      "resPath": "tables/learningData.csv",
      "resType": "table",
      "resFormat": [
        "text/csv"
      ],
      "isCollection": false,
      "columns": [
        {
          "colIndex": 0,
          "colName": "d3mIndex",
          "colType": "integer",
          "role": [
            "index"
          ]
        },
        {
          "colIndex": 1,
          "colName": "labels",
          "colType": "text",
          "role": [
            "attribute"
          ]
        },
        {
          "colIndex": 2,
          "colName": "probabilities",
          "colType": "text",
          "role": [
            "attribute"
          ]
        }
      ]
    }
  ]
}`
	schemaOutput = fmt.Sprintf(schemaOutput, schema.Properties.DatasetID)
	err = ioutil.WriteFile(outputSchemaPath, []byte(schemaOutput), 0644)
	if err != nil {
		return "", errors.Wrap(err, "error writing classification schema")
	}

	absResultDir, err := filepath.Abs(resultDir)
	if err != nil {
		return "", errors.Wrap(err, "Failed to generate absolute path")
	}

	return absResultDir, nil
}

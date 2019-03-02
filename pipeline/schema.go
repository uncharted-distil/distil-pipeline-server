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
	"encoding/json"
	"io/ioutil"
	"path"

	"github.com/pkg/errors"
)

// DataSchema encapsulates the data schema json structure.
type DataSchema struct {
	Properties    *DataSchemaProperties `json:"about"`
	DataResources []*DataResource       `json:"dataResources"`
}

// DataSchemaProperties contains the basic properties of a dataset.
type DataSchemaProperties struct {
	DatasetID       string `json:"datasetID"`
	DatasetName     string `json:"datasetName"`
	Description     string `json:"description"`
	Citation        string `json:"citation"`
	License         string `json:"license"`
	Source          string `json:"source"`
	SourceURI       string `json:"sourceURI"`
	ApproximateSize string `json:"approximateSize"`
	Redacted        bool   `json:"redacted"`
	SchemaVersion   string `json:"datasetSchemaVersion"`
}

// DataResource represents a set of variables.
type DataResource struct {
	ResID        string          `json:"resID"`
	ResPath      string          `json:"resPath"`
	ResType      string          `json:"resType"`
	ResFormat    []string        `json:"resFormat"`
	IsCollection bool            `json:"isCollection"`
	Variables    []*DataVariable `json:"columns"`
}

// DataVariable captures the data schema representation of a variable.
type DataVariable struct {
	ColName  string   `json:"colName"`
	Role     []string `json:"role"`
	ColType  string   `json:"colType"`
	ColIndex int      `json:"colIndex"`
}

// LoadDataSchema reads the schema file and loads the struct.
func loadDataSchema(dataPath string) (*DataSchema, error) {
	schemaFile := path.Join(dataPath, "datasetDoc.json")
	b, err := ioutil.ReadFile(schemaFile)
	if err != nil {
		return nil, errors.Wrap(err, "unable to read schema file")
	}

	schema := &DataSchema{}
	err = json.Unmarshal(b, schema)
	if err != nil {
		return nil, errors.Wrap(err, "unable to parse schema file")
	}

	return schema, nil
}

package client

import (
	"testing"
	"time"
	"encoding/json"

	"github.com/stretchr/testify/assert"
	"github.com/influxdata/influxdb/client/v2"
)

const database = "test_database"

func TestSendMetrics(t *testing.T) {
	influxDb, err := NewAsyncClient(&AsyncClientConfig{
		Endpoint: "http://localhost:8086",
		Database: database,
		BatchSize: 100,
		FlushTimeout: 1 * time.Second,
	})

	drop("test1", influxDb)
	assert.NotNil(t, influxDb)
	assert.Nil(t, err)

	for i := 0; i < 100; i++ {
		err = influxDb.send(makeMetricDatum("test1"))
		assert.Nil(t, err)
	}
	time.Sleep(200 * time.Millisecond)

	result, err := influxDb.client.Query(client.Query{
		Command: "select count(*) from test1",
		Database: database,
	})
	assert.Nil(t, err)

	assert.Equal(t, int64(100), getCount(result))
}

func TestSendMetricsAfterTimeout(t *testing.T) {
	influxDb, err := NewAsyncClient(&AsyncClientConfig{
		Endpoint: "http://localhost:8086",
		Database: database,
		BatchSize: 10,
		FlushTimeout: 100 * time.Millisecond,
	})

	drop("test2", influxDb)
	assert.NotNil(t, influxDb)
	assert.Nil(t, err)

	err = influxDb.send(makeMetricDatum("test2"))
	time.Sleep(150 * time.Millisecond)

	result, err := influxDb.client.Query(client.Query{
		Command: "select count(*) from test2",
		Database: database,
	})
	assert.Nil(t, err)

	assert.Equal(t, int64(1), getCount(result))
}

func getCount(influxResponse *client.Response) int64 {
	count := influxResponse.Results[0].Series[0].Values[0][1].(json.Number)
	countValue, _ := count.Int64()
	return countValue

}

func drop(measurement string, asyncClient *AsyncClient) {
	asyncClient.client.Query(client.Query{
		Command: "drop measurement " + measurement,
		Database: database,
	})
}

func makeMetricDatum(measurement string) *MetricDatum {
	tags := map[string]string{"type": "conversion"}
	fields := map[string]interface{}{ "count": 1 }
	return NewMetricDatum(measurement, tags, fields)
}

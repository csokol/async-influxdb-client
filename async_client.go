package asyncinflux

import (
	"github.com/influxdata/influxdb/client/v2"
	"time"
	"errors"
	"log"
)

type MetricDatum struct {
	measurement string
	tags map[string]string
	fields map[string]interface{}
	time time.Time
}

func NewMetricDatum(measurement string, tags map[string]string, fields map[string]interface{}) *MetricDatum {
	return &MetricDatum {
		measurement: measurement,
		tags: tags,
		fields: fields,
		time: time.Now(),
	}
}

type AsyncClient struct {
	client  client.Client
	database string
	batchSize int
	pointsChannel chan *client.Point
	batchConfig client.BatchPointsConfig
}

type AsyncClientConfig struct {
	Endpoint string
	Database string
	BatchSize int
	FlushTimeout time.Duration
}

func (influxDb *AsyncClient) Send(metric *MetricDatum) {
	if err := influxDb.send(metric); err != nil {
		log.Printf("Could not send metric to influx: %s\n", err.Error())
	}
}

func (influxDb *AsyncClient) send(metric *MetricDatum) error {
	if influxDb == nil {
		return errors.New("Failed to create influxdb client")
	}

	pt, err := client.NewPoint(metric.measurement, metric.tags, metric.fields, metric.time)
	if err != nil {
		return err
	}

	influxDb.pointsChannel <- pt
	return nil
}

func (influxDb *AsyncClient) batchFlusher(points chan *client.Point, ticker *time.Ticker) {
	pointsBuffer := make([]*client.Point, influxDb.batchSize)
	currentBatchSize := 0
	for {
		select {
			case <- ticker.C:
				influxDb.flush(pointsBuffer, currentBatchSize)
				currentBatchSize = 0
			case point := <- points:
				pointsBuffer[currentBatchSize] = point
				currentBatchSize++
				if influxDb.batchSize == currentBatchSize {
					influxDb.flush(pointsBuffer, currentBatchSize)
					currentBatchSize = 0
				}
		}
	}
}

func (influxDb *AsyncClient) flush(points []*client.Point, size int) {
	newBatch, _ := client.NewBatchPoints(influxDb.batchConfig)
	newBatch.AddPoints(points[0:size])
	influxDb.client.Write(newBatch)
}

func NewAsyncClient(config *AsyncClientConfig) (*AsyncClient, error) {
	httpConfig := client.HTTPConfig { Addr: config.Endpoint, Timeout: 1 * time.Second }
	influxdbClient, err := client.NewHTTPClient(httpConfig)
	if err != nil {
		return nil, err
	}
	asyncClient := &AsyncClient{
		client: influxdbClient,
		database: config.Database,
		batchSize: config.BatchSize,
		pointsChannel: make(chan *client.Point, config.BatchSize * 50),
		batchConfig: client.BatchPointsConfig {Database: config.Database},
	}
	go asyncClient.batchFlusher(asyncClient.pointsChannel, time.NewTicker(config.FlushTimeout))
	return asyncClient, nil
}

func DefaultClient(influxEndpoint string, influxDb string) (*AsyncClient, error) {
	return NewAsyncClient(&AsyncClientConfig {
		Endpoint: influxEndpoint,
		Database: influxDb,
		BatchSize: 100,
		FlushTimeout: 500 * time.Millisecond,
	})
}

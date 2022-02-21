package streams

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/outputs/codec"
	"github.com/elastic/beats/libbeat/outputs/codec/json"
	"github.com/elastic/beats/libbeat/publisher"
	"github.com/jpillora/backoff"
)

var MAX_RECORD_SIZE = 1024*1024 - 300

type client struct {
	streams              kinesisStreamsClient
	streamName           string
	partitionKeyProvider PartitionKeyProvider
	beatName             string
	encoder              codec.Codec
	timeout              time.Duration
	batchSizeBytes       int
	observer             outputs.Observer
	backoff              backoff.Backoff
	gzip                 bool
}

type batch struct {
	okEvents [][]publisher.Event
	records  []*kinesis.PutRecordsRequestEntry
	dropped  int
}

type kinesisStreamsClient interface {
	PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error)
}

func newClient(sess *session.Session, config *StreamsConfig, observer outputs.Observer, beat beat.Info) (*client, error) {
	partitionKeyProvider := createPartitionKeyProvider(config)
	client := &client{
		streams:              kinesis.New(sess),
		streamName:           config.DeliveryStreamName,
		partitionKeyProvider: partitionKeyProvider,
		beatName:             beat.Beat,
		encoder:              json.New(beat.Version, json.Config{Pretty: false, EscapeHTML: true}),
		timeout:              config.Timeout,
		batchSizeBytes:       config.BatchSizeBytes,
		observer:             observer,
		backoff:              config.Backoff,
		gzip:                 config.GZip,
	}

	return client, nil
}

func createPartitionKeyProvider(config *StreamsConfig) PartitionKeyProvider {
	if config.PartitionKeyProvider == "xid" {
		return newXidPartitionKeyProvider()
	}
	return newFieldPartitionKeyProvider(config.PartitionKey)
}

func (client client) String() string {
	return "streams"
}

func (client *client) Close() error {
	return nil
}

func (client *client) Connect() error {
	return nil
}

func (client *client) Publish(batch publisher.Batch) error {
	events := batch.Events()
	rest, _ := client.publishEvents(events)
	if len(rest) == 0 {
		// We have to ACK only when all the submission succeeded
		// Ref: https://github.com/elastic/beats/blob/c4af03c51373c1de7daaca660f5d21b3f602771c/libbeat/outputs/elasticsearch/client.go#L232
		batch.ACK()
	} else {
		// Mark the failed events to retry
		// Ref: https://github.com/elastic/beats/blob/c4af03c51373c1de7daaca660f5d21b3f602771c/libbeat/outputs/elasticsearch/client.go#L234
		batch.RetryEvents(rest)
	}
	// This shouldn't be an error object according to other official beats' implementations
	// Ref: https://github.com/elastic/beats/blob/c4af03c51373c1de7daaca660f5d21b3f602771c/libbeat/outputs/kafka/client.go#L119
	return nil
}

func (client *client) publishEvents(events []publisher.Event) ([]publisher.Event, error) {
	var batches []batch
	if client.gzip { // maybe do single compnent and compose it with relevant mapEvents lets see if we do more features like this
		batches = client.mapEventsGZip(events)
	} else {
		batches = client.mapEvents(events)
	}
	totalFailed := []publisher.Event{}
	for _, batch := range batches {
		failed, err := client.publishBatch(batch)
		if err != nil || len(failed) > 0 {
			totalFailed = append(totalFailed, failed...)
		}
	}
	logp.Debug("kinesis", "received batches: %d for events %d", len(batches), len(events))
	return totalFailed, nil
}

func (client *client) publishBatch(b batch) ([]publisher.Event, error) {
	observer := client.observer

	okEvents, records, dropped := b.okEvents, b.records, b.dropped
	observer.NewBatch(client.lenListOfLists(okEvents))
	if dropped > 0 {
		logp.Debug("kinesis", "sent %d records: %v", len(records), records)
		observer.Dropped(dropped)
		observer.Acked(client.lenListOfLists(okEvents))
		if len(records) == 0 {
			logp.Debug("kinesis", "No records were mapped")
			return nil, nil
		}
	}
	logp.Debug("kinesis", "mapped to records: %v", records)
	res, err := client.putKinesisRecords(records)
	failed := collectFailedEvents(res, okEvents)
	if len(failed) == 0 {
		if aerr, ok := err.(awserr.Error); ok {
			switch aerr.Code() {
			case kinesis.ErrCodeLimitExceededException:
			case kinesis.ErrCodeProvisionedThroughputExceededException:
			case kinesis.ErrCodeInternalFailureException:
				logp.Info("putKinesisRecords failed (api level, not per-record failure). Will retry all records. error: %v", err)
				failed = client.eventsFlattenLists(okEvents)
			default:
				logp.Warn("putKinesisRecords persistent failure. Will not retry. error: %v", err)
			}
		}
	}
	if err != nil || len(failed) > 0 {
		dur := client.backoff.Duration()
		logp.Info("retrying %d events client.backoff.Duration %s on error: %v", len(failed), dur, err)
		time.Sleep(dur)
	} else {
		client.backoff.Reset()
	}
	return failed, err
}
func (client *client) eventsFlattenLists(listOfListsEvents [][]publisher.Event) []publisher.Event {
	var flatEvents []publisher.Event
	for _, events := range listOfListsEvents {
		flatEvents = append(flatEvents, events...)
	}
	return flatEvents
}

func (client *client) lenListOfLists(listOfListsEvents [][]publisher.Event) int {
	var lenList int
	for _, events := range listOfListsEvents {
		lenList += len(events)
	}
	return lenList
}

func (client *client) mapEvents(events []publisher.Event) []batch {
	batches := []batch{}
	dropped := 0
	records := []*kinesis.PutRecordsRequestEntry{}
	okEvents := []publisher.Event{}

	batchSize := 0

	for i := range events {
		event := events[i]
		size, record, err := client.mapEvent(&event)
		if size >= client.batchSizeBytes || size >= MAX_RECORD_SIZE {
			logp.Critical("kinesis single record of size %d is bigger than batchSizeBytes %d or bigger than kinesis max record size %d, sending batch without it! no backoff!", size, client.batchSizeBytes, MAX_RECORD_SIZE)
			continue
		}
		if err != nil {
			logp.Debug("kinesis", "failed to map event(%v): %v", event, err)
			dropped++
		} else if batchSize+size >= client.batchSizeBytes {
			batches = append(batches, batch{
				okEvents: client.eventsToListOfLists(okEvents),
				records:  records,
				dropped:  dropped})
			dropped = 0
			batchSize = size
			records = []*kinesis.PutRecordsRequestEntry{record}
			okEvents = []publisher.Event{event}
		} else {
			batchSize += size
			okEvents = append(okEvents, event)
			records = append(records, record)
		}
	}
	batches = append(batches, batch{
		okEvents: client.eventsToListOfLists(okEvents),
		records:  records,
		dropped:  dropped})
	return batches
}

func (client *client) eventsToListOfLists(events []publisher.Event) [][]publisher.Event {
	var lists [][]publisher.Event
	for i := range events {
		lists = append(lists, []publisher.Event{events[i]})
	}
	return lists
}

func (client *client) mapEvent(event *publisher.Event) (int, *kinesis.PutRecordsRequestEntry, error) {
	var buf []byte
	{
		serializedEvent, err := client.encoder.Encode(client.beatName, &event.Content)
		if err != nil {
			logp.Critical("Unable to encode event: %v", err)
			return 0, nil, err
		}
		// See https://github.com/elastic/beats/blob/5a6630a8bc9b9caf312978f57d1d9193bdab1ac7/libbeat/outputs/kafka/client.go#L163-L164
		// You need to copy the byte data like this. Otherwise you see strange issues like all the records sent in a same batch has the same Data.
		buf = make([]byte, len(serializedEvent)+1)
		copy(buf, serializedEvent)
		// Firehose doesn't automatically add trailing new-line on after each record.
		// This ends up a stream->firehose->s3 pipeline to produce useless s3 objects.
		// No ndjson, but a sequence of json objects without separators...
		// Fix it just adding a new-line.
		//
		// See https://stackoverflow.com/questions/43010117/writing-properly-formatted-json-to-s3-to-load-in-athena-redshift
		buf[len(buf)-1] = byte('\n')
	}

	partitionKey, err := client.partitionKeyProvider.PartitionKeyFor(event)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get parititon key: %v", err)
	}

	return len(buf), &kinesis.PutRecordsRequestEntry{Data: buf, PartitionKey: aws.String(partitionKey)}, nil
}

func (client *client) mapEventsGZip(events []publisher.Event) []batch {
	batches := []batch{}
	dropped := 0
	records := []*kinesis.PutRecordsRequestEntry{}
	okEvents := [][]publisher.Event{}
	toGZIPBytes := [][]byte{}

	toGZIPSize := 0
	batchSize := 0

	for i := range events {
		var gzippedBytes []byte
		event := events[i]
		buf, err := client.getBytesFromEvent(&event)
		if err != nil {
			logp.Debug("kinesis", "failed to get bytes from event(%v): %v", event, err)
			dropped++
			continue
		}
		size := len(buf)
		if size >= client.batchSizeBytes || size >= MAX_RECORD_SIZE { // single record is bigger than batchSizeBytes or bigger than kinesis max record size
			gzippedBytes, err = client.getGZipFormatedBytes([][]byte{buf})
			if err != nil {
				logp.Critical("Unable to gzip event: %v", err)
				dropped++
				continue
			}
			size = len(gzippedBytes)
			if size >= client.batchSizeBytes || size >= MAX_RECORD_SIZE { // gzipped single record is bigger than batchSizeBytes or bigger than kinesis max record size
				logp.Critical("kinesis single record of size:%d  was gzipped to size %d is bigger than batchSizeBytes %d or bigger than kinesis max record size %d, sending batch without it! no backoff!",
					len(buf), size, client.batchSizeBytes, MAX_RECORD_SIZE)
				dropped++
				continue
			}
			_, record, err := client.mapBytes(gzippedBytes, &event) // TODO: add +1 to size and remove size here and in mapBytes
			if err != nil {
				logp.Debug("kinesis", "failed to map event from bytes(%v): %v", event, err)
				dropped++
				continue
			}
			if batchSize+size >= client.batchSizeBytes {
				batches = append(batches, batch{
					okEvents: okEvents,
					records:  records,
					dropped:  dropped})
				dropped = 0
				batchSize = size
				records = []*kinesis.PutRecordsRequestEntry{record}
				okEvents = [][]publisher.Event{{event}}
			} else { // prepand event and record to be able to add more events to previous toGZIPBytes and keep records and okEvents in sync
				batchSize += size
				okEvents = append([][]publisher.Event{{event}}, okEvents...)
				records = append([]*kinesis.PutRecordsRequestEntry{record}, records...)
			}
		} else {
			if toGZIPSize+size >= client.batchSizeBytes || toGZIPSize+size >= MAX_RECORD_SIZE {
				gzippedBytes, err = client.getGZipFormatedBytes(toGZIPBytes)
				if err != nil {
					logp.Critical("Unable to gzip event: %v", err)
					dropped += len(toGZIPBytes)
					continue
				}
				toGZIPSize = size
				toGZIPBytes = [][]byte{buf}
				// fill batch
				_, record, err := client.mapBytes(gzippedBytes, &event) // TODO: add +1 to size and remove size here and in mapBytes
				if err != nil {
					logp.Debug("kinesis", "failed to map event from bytes(%v): %v", event, err)
					dropped += len(toGZIPBytes)
					continue
				}
				sizeAfterGZIP := len(gzippedBytes)
				if batchSize+sizeAfterGZIP >= client.batchSizeBytes {
					batches = append(batches, batch{
						okEvents: okEvents[:len(okEvents)-1], // remove last event from okEvents
						records:  records,
						dropped:  dropped})
					dropped = 0
					batchSize = sizeAfterGZIP
					records = []*kinesis.PutRecordsRequestEntry{record}
					okEvents = [][]publisher.Event{okEvents[len(okEvents)-1]}
				} else {
					batchSize += sizeAfterGZIP
					okEvents = append(okEvents, []publisher.Event{event})
					records = append(records, record)
				}
			} else { // fill the data to be gzipped
				if len(okEvents) == 0 {
					okEvents = [][]publisher.Event{{event}}
				} else {
					okEvents[len(okEvents)-1] = append(okEvents[len(okEvents)-1], event) // append event to last okEvents
				}
				toGZIPBytes = append(toGZIPBytes, buf)
				toGZIPSize += size
			}
		}
	}
	// fill last batch
	if len(toGZIPBytes) > 0 {
		gzippedBytes, err := client.getGZipFormatedBytes(toGZIPBytes)
		if err != nil {
			logp.Critical("Unable to gzip event: %v", err)
			dropped += len(toGZIPBytes)
			return append(batches, batch{
				okEvents: okEvents,
				records:  records,
				dropped:  dropped})
		}
		_, record, err := client.mapBytes(gzippedBytes, &events[len(events)-1]) // TODO: add +1 to size and remove size here and in mapBytes
		if err != nil {
			logp.Debug("kinesis", "failed to map event from bytes(%v): %v", events[len(events)-1], err)
			dropped += len(toGZIPBytes)
			return append(batches, batch{
				okEvents: okEvents,
				records:  records,
				dropped:  dropped})
		}
		records = append(records, record)
		okEvents = append(okEvents, []publisher.Event{events[len(events)-1]})
	}
	batches = append(batches, batch{
		okEvents: okEvents,
		records:  records,
		dropped:  dropped})
	return batches
}

func (client *client) getBytesFromEvent(event *publisher.Event) ([]byte, error) {
	var buf []byte
	{
		serializedEvent, err := client.encoder.Encode(client.beatName, &event.Content)
		if err != nil {
			logp.Critical("Unable to encode event: %v", err)
			return nil, err
		}
		// See https://github.com/elastic/beats/blob/5a6630a8bc9b9caf312978f57d1d9193bdab1ac7/libbeat/outputs/kafka/client.go#L163-L164
		// You need to copy the byte data like this. Otherwise you see strange issues like all the records sent in a same batch has the same Data.
		buf = make([]byte, len(serializedEvent))
		copy(buf, serializedEvent)
	}

	return buf, nil
}

func (client *client) getGZipFormatedBytes(eventsBytes [][]byte) ([]byte, error) {
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gzipData := []byte("[")
	gzipData = append(gzipData, bytes.Join(eventsBytes, []byte{','})...)
	gzipData = append(gzipData, byte(']'))
	if _, err := gz.Write(gzipData); err != nil {
		return nil, fmt.Errorf("failed to write gzip: %v", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("failed to close gzip: %v", err)
	}
	b64GZIP := base64.StdEncoding.EncodeToString(buf.Bytes())
	gzipDataFormated := []byte(fmt.Sprintf("{\"kinesis_event_type\":\"JSON_GZIP_BULK\",\"data\":\"%s\"}", b64GZIP))
	// the code commented below is for testing purpose mimicking same compression ratio
	// pref := []byte{}
	// for _, buf := range eventsBytes {
	// 	pref = append(pref, buf[0])
	// }
	// stars := strings.Repeat("*", len(b64GZIP)-len(pref))
	// gzipDataFormated = []byte(fmt.Sprintf("{\"kinesis_event_type\":\"JSON_GZIP_BULK\",\"data\":\"%s%s\"}", string(pref), stars))
	return gzipDataFormated, nil
}

func (client *client) mapBytes(buf []byte, event *publisher.Event) (int, *kinesis.PutRecordsRequestEntry, error) {
	var bufCP []byte
	{
		// See https://github.com/elastic/beats/blob/5a6630a8bc9b9caf312978f57d1d9193bdab1ac7/libbeat/outputs/kafka/client.go#L163-L164
		// You need to copy the byte data like this. Otherwise you see strange issues like all the records sent in a same batch has the same Data.
		bufCP = make([]byte, len(buf)+1)
		copy(bufCP, buf)
		// Firehose doesn't automatically add trailing new-line on after each record.
		// This ends up a stream->firehose->s3 pipeline to produce useless s3 objects.
		// No ndjson, but a sequence of json objects without separators...
		// Fix it just adding a new-line.
		//
		// See https://stackoverflow.com/questions/43010117/writing-properly-formatted-json-to-s3-to-load-in-athena-redshift
		bufCP[len(bufCP)-1] = byte('\n')
	}
	partitionKey, err := client.partitionKeyProvider.PartitionKeyFor(event)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to get parititon key: %v", err)
	}

	return len(buf), &kinesis.PutRecordsRequestEntry{Data: bufCP, PartitionKey: aws.String(partitionKey)}, nil
}

func (client *client) putKinesisRecords(records []*kinesis.PutRecordsRequestEntry) (*kinesis.PutRecordsOutput, error) {
	request := kinesis.PutRecordsInput{
		StreamName: &client.streamName,
		Records:    records,
	}
	res, err := client.streams.PutRecords(&request)
	if err != nil {
		return res, fmt.Errorf("failed to put records: %v", err)
	}
	return res, nil
}

func collectFailedEvents(res *kinesis.PutRecordsOutput, events [][]publisher.Event) []publisher.Event {
	if res.FailedRecordCount != nil && *res.FailedRecordCount > 0 {
		failedEvents := make([]publisher.Event, 0)
		records := res.Records
		for i, r := range records {
			if r == nil {
				// See https://github.com/s12v/awsbeats/issues/27 for more info
				logp.NewLogger("streams").Warn("no record returned from kinesis for event: ", events[i])
				continue
			}
			if r.ErrorCode == nil {
				// logp.NewLogger("streams").Warn("skipping failed event with unexpected state: corresponding kinesis record misses error code: ", r)
				continue
			}
			if *r.ErrorCode == "ProvisionedThroughputExceededException" {
				logp.NewLogger("streams").Debug("throughput exceeded. will retry", r)
				failedEvents = append(failedEvents, events[i]...)
			}
			if *r.ErrorCode != "" {
				failedEvents = append(failedEvents, events[i]...)
			}
		}
		logp.Warn("Retrying %d events", len(failedEvents))
		return failedEvents
	}
	return []publisher.Event{}
}

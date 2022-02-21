package streams

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/kinesis"
	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/outputs"
	"github.com/elastic/beats/libbeat/publisher"
)

type StubCodec struct {
	dat [][]byte
	err []error
}

func (c *StubCodec) Encode(index string, event *beat.Event) ([]byte, error) {
	dat, err := c.dat[0], c.err[0]
	c.dat = c.dat[1:]
	c.err = c.err[1:]
	return dat, err
}

type StubClient struct {
	calls []*kinesis.PutRecordsInput
	out   []*kinesis.PutRecordsOutput
	err   []error
}

func (c *StubClient) PutRecords(input *kinesis.PutRecordsInput) (*kinesis.PutRecordsOutput, error) {
	c.calls = append(c.calls, input)
	out, err := c.out[0], c.err[0]
	c.out = c.out[1:]
	c.err = c.err[1:]
	return out, err
}

func TestCreateXidPartitionKeyProvider(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	config := &StreamsConfig{PartitionKeyProvider: "xid"}
	event := &publisher.Event{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}}

	xidProvider := createPartitionKeyProvider(config)
	xidKey, err := xidProvider.PartitionKeyFor(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if xidKey == "" || xidKey == expectedPartitionKey {
		t.Fatalf("uenxpected partition key: %s", xidKey)
	}
}

func TestCreateFieldPartitionKeyProvider(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	config := &StreamsConfig{PartitionKey: fieldForPartitionKey}
	event := &publisher.Event{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}}
	fieldProvider := createPartitionKeyProvider(config)
	fieldKey, err := fieldProvider.PartitionKeyFor(event)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if fieldKey != expectedPartitionKey {
		t.Fatalf("uenxpected partition key: %s", fieldKey)

	}
}

func TestMapEvent(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)
	codecData := [][]byte{[]byte("boom")}
	codecErr := []error{nil}
	client := &client{encoder: &StubCodec{dat: codecData, err: codecErr}, partitionKeyProvider: provider}
	event := &publisher.Event{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}}
	_, record, err := client.mapEvent(event)

	if err != nil {
		t.Fatalf("uenxpected error: %v", err)
	}

	if string(record.Data) != "boom\n" {
		t.Errorf("Unexpected data: %s", record.Data)
	}

	actualPartitionKey := aws.StringValue(record.PartitionKey)
	if actualPartitionKey != expectedPartitionKey {
		t.Errorf("unexpected partition key: %s", actualPartitionKey)
	}
}

func TestMapEventsEventBiggetThanMaxSizeOfRecord(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)

	codecData := [][]byte{[]byte("boom"), []byte("boo")}
	codecErr := []error{nil, nil}
	origMaxSizeOfRecord := MAX_RECORD_SIZE
	MAX_RECORD_SIZE = 5
	client := client{
		encoder:              &StubCodec{dat: codecData, err: codecErr},
		partitionKeyProvider: provider,
		batchSizeBytes:       100,
	}
	events := []publisher.Event{
		{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}},
		{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}},
	}
	batches := client.mapEvents(events)
	okEvents, records, dropped := batches[0].okEvents, batches[0].records, batches[0].dropped

	if dropped != 0 {
		t.Errorf("Expected 0 dropped, got: %d", dropped)
	}
	if len(records) != 1 {
		t.Errorf("Expected 1 records, got %v", len(records))
	}
	if len(okEvents) != 1 {
		t.Errorf("Expected 1 ok events, got %v", len(okEvents))
	}
	if string(records[0].Data) != "boo\n" {
		t.Errorf("Unexpected data %s", records[0].Data)
	}
	MAX_RECORD_SIZE = origMaxSizeOfRecord // cleanup
}

func TestMapEventsEventBiggetThanMaxSize(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)

	codecData := [][]byte{[]byte("boom"), []byte("boo")}
	codecErr := []error{nil, nil}
	client := client{
		encoder:              &StubCodec{dat: codecData, err: codecErr},
		partitionKeyProvider: provider,
		batchSizeBytes:       5,
	}
	events := []publisher.Event{
		{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}},
		{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}},
	}
	batches := client.mapEvents(events)
	okEvents, records, dropped := batches[0].okEvents, batches[0].records, batches[0].dropped

	if dropped != 0 {
		t.Errorf("Expected 0 dropped, got: %d", dropped)
	}
	if len(records) != 1 {
		t.Errorf("Expected 1 records, got %v", len(records))
	}
	if len(okEvents) != 1 {
		t.Errorf("Expected 1 ok events, got %v", len(okEvents))
	}
	if string(records[0].Data) != "boo\n" {
		t.Errorf("Unexpected data %s", records[0].Data)
	}

}

func TestMapEvents(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)

	codecData := [][]byte{[]byte("boom")}
	codecErr := []error{nil}
	client := client{
		encoder:              &StubCodec{dat: codecData, err: codecErr},
		partitionKeyProvider: provider,
		batchSizeBytes:       5 * 1000 * 1000,
	}
	event := publisher.Event{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}}
	events := []publisher.Event{event}
	batches := client.mapEvents(events)
	okEvents, records, _ := batches[0].okEvents, batches[0].records, batches[0].dropped

	if len(records) != 1 {
		t.Errorf("Expected 1 records, got %v", len(records))
	}

	if len(okEvents) != 1 {
		t.Errorf("Expected 1 ok events, got %v", len(okEvents))
	}

	if string(records[0].Data) != "boom\n" {
		t.Errorf("Unexpected data %s", records[0].Data)
	}
}

func TestPublishEvents(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	expectedPartitionKey := "foobar"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)
	client := client{
		partitionKeyProvider: provider,
		observer:             outputs.NewNilObserver(),
		batchSizeBytes:       5 * 1000 * 1000,
	}
	event := publisher.Event{Content: beat.Event{Fields: common.MapStr{fieldForPartitionKey: expectedPartitionKey}}}
	events := []publisher.Event{event}

	{
		codecData := [][]byte{[]byte("boom")}
		codecErr := []error{nil}
		client.encoder = &StubCodec{dat: codecData, err: codecErr}

		putRecordsOut := []*kinesis.PutRecordsOutput{
			&kinesis.PutRecordsOutput{
				Records: []*kinesis.PutRecordsResultEntry{
					&kinesis.PutRecordsResultEntry{
						ErrorCode: aws.String(""),
					},
				},
				FailedRecordCount: aws.Int64(0),
			},
		}
		putRecordsErr := []error{nil}
		client.streams = &StubClient{out: putRecordsOut, err: putRecordsErr}
		rest, err := client.publishEvents(events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(rest) != 0 {
			t.Errorf("unexpected number of remaining events: %d", len(rest))
		}
	}

	{
		// An event that can't be encoded should be ignored without any error, but with some log.
		codecData := [][]byte{[]byte("")}
		codecErr := []error{fmt.Errorf("failed to encode")}
		client.encoder = &StubCodec{dat: codecData, err: codecErr}
		putRecordsOut := []*kinesis.PutRecordsOutput{
			&kinesis.PutRecordsOutput{
				Records: []*kinesis.PutRecordsResultEntry{
					&kinesis.PutRecordsResultEntry{
						ErrorCode: aws.String(""),
					},
				},
				FailedRecordCount: aws.Int64(0),
			},
		}
		putRecordsErr := []error{nil}
		client.streams = &StubClient{out: putRecordsOut, err: putRecordsErr}

		rest, err := client.publishEvents(events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(rest) != 0 {
			t.Errorf("unexpected number of remaining events: %d", len(rest))
		}
	}

	{
		// Nil records returned by Kinesis should be ignored with some log
		codecData := [][]byte{[]byte("boom")}
		codecErr := []error{nil}
		client.encoder = &StubCodec{dat: codecData, err: codecErr}

		putRecordsOut := []*kinesis.PutRecordsOutput{
			&kinesis.PutRecordsOutput{
				Records: []*kinesis.PutRecordsResultEntry{
					nil,
				},
				FailedRecordCount: aws.Int64(1),
			},
		}
		putRecordsErr := []error{nil}
		client.streams = &StubClient{out: putRecordsOut, err: putRecordsErr}

		rest, err := client.publishEvents(events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(rest) != 0 {
			t.Errorf("unexpected number of remaining events: %d", len(rest))
		}
	}

	{
		// Records with nil error codes should be ignored with some log
		codecData := [][]byte{[]byte("boom")}
		codecErr := []error{nil}
		client.encoder = &StubCodec{dat: codecData, err: codecErr}

		putRecordsOut := []*kinesis.PutRecordsOutput{
			&kinesis.PutRecordsOutput{
				Records: []*kinesis.PutRecordsResultEntry{
					&kinesis.PutRecordsResultEntry{
						ErrorCode: nil,
					},
				},
				FailedRecordCount: aws.Int64(1),
			},
		}
		putRecordsErr := []error{nil}
		client.streams = &StubClient{out: putRecordsOut, err: putRecordsErr}

		rest, err := client.publishEvents(events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(rest) != 0 {
			t.Errorf("unexpected number of remaining events: %d", len(rest))
		}
	}

	{
		// Kinesis received the event but it was not persisted, probably due to underlying infrastructure failure
		codecData := [][]byte{[]byte("boom")}
		codecErr := []error{nil}
		client.encoder = &StubCodec{dat: codecData, err: codecErr}

		putRecordsOut := []*kinesis.PutRecordsOutput{
			&kinesis.PutRecordsOutput{
				Records: []*kinesis.PutRecordsResultEntry{
					&kinesis.PutRecordsResultEntry{
						ErrorCode: aws.String("simulated_error"),
					},
				},
				FailedRecordCount: aws.Int64(1),
			},
		}
		putRecordsErr := []error{nil}
		client.streams = &StubClient{out: putRecordsOut, err: putRecordsErr}

		rest, err := client.publishEvents(events)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(rest) != 1 {
			t.Errorf("unexpected number of remaining events: %d", len(rest))
		}
	}
}

func TestTestPublishEventsBatch(t *testing.T) {
	events := []publisher.Event{}
	fieldForPartitionKey := "mypartitionkey"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)
	client := client{
		partitionKeyProvider: provider,
		observer:             outputs.NewNilObserver(),
		batchSizeBytes:       5 * 1000 * 1000,
	}
	codecData := [][]byte{
		[]byte(strings.Repeat("a", 500000)),
		[]byte(strings.Repeat("a", 500000)),
		[]byte(strings.Repeat("a", 500000)),
		[]byte(strings.Repeat("a", 900000)),
		[]byte(strings.Repeat("a", 900000)),
		[]byte(strings.Repeat("a", 900000)),
		[]byte(strings.Repeat("a", 900000)),
		[]byte(strings.Repeat("a", 900000)),
		[]byte(strings.Repeat("a", 900000)),
	}
	codecErr := []error{
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	}
	client.encoder = &StubCodec{dat: codecData, err: codecErr}

	putRecordsOutputGood := &kinesis.PutRecordsOutput{
		Records: []*kinesis.PutRecordsResultEntry{
			&kinesis.PutRecordsResultEntry{
				ErrorCode: aws.String(""),
			},
		},
		FailedRecordCount: aws.Int64(0),
	}
	putRecordsOut := []*kinesis.PutRecordsOutput{
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
		putRecordsOutputGood,
	}
	putRecordsErr := []error{
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
	}
	kinesisStub := &StubClient{out: putRecordsOut, err: putRecordsErr}
	client.streams = kinesisStub

	for _, _ = range putRecordsErr {
		events = append(events, publisher.Event{
			Content: beat.Event{
				Fields: common.MapStr{
					fieldForPartitionKey: "expectedPartitionKey",
				},
			},
		},
		)
	}
	rest, err := client.publishEvents(events)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rest) != 0 {
		t.Errorf("unexpected number of remaining events: %d", len(rest))
	}
	if len(kinesisStub.calls) != 2 {
		t.Errorf("unexpected number of batches: %d", len(kinesisStub.calls))
	}
	if len(kinesisStub.calls[0].Records) != 6 {
		t.Errorf("unexpected number of events in batch 0 batches: %d", len(kinesisStub.calls[0].Records))
	}
	if len(kinesisStub.calls[1].Records) != 3 {
		t.Errorf("unexpected number of events in batch 1 batches: %d", len(kinesisStub.calls[1].Records))
	}
}

func TestTestPublishEventsBatchGZIP(t *testing.T) {
	events := []publisher.Event{}
	fieldForPartitionKey := "mypartitionkey"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)
	client := client{
		partitionKeyProvider: provider,
		observer:             outputs.NewNilObserver(),
		batchSizeBytes:       1500,
		gzip:                 true,
	}
	origMaxSizeOfRecord := MAX_RECORD_SIZE
	MAX_RECORD_SIZE = 500
	codecData := [][]byte{
		[]byte(strings.Repeat("a", 100)),
		[]byte(strings.Repeat("b", 150)),
		[]byte(strings.Repeat("c", 600)),
		[]byte(strings.Repeat("d", 100)),
		[]byte(strings.Repeat("e", 100)),
		[]byte(strings.Repeat("f", 100)),
		[]byte(strings.Repeat("g", 300)),
		[]byte(strings.Repeat("h", 200)),
		[]byte(strings.Repeat("i", 100)),
		[]byte(strings.Repeat("j", 100)),
		[]byte(strings.Repeat("k", 200)),
		[]byte(strings.Repeat("l", 100)),
		[]byte(strings.Repeat("m", 200)),
		[]byte(strings.Repeat("n", 160)),
		[]byte(strings.Repeat("o", 220)),
		[]byte(strings.Repeat("p", 400)),
		[]byte(strings.Repeat("q", 100)),
		[]byte(strings.Repeat("r", 200)),
		[]byte(strings.Repeat("s", 400)),
		[]byte(strings.Repeat("t", 100)),
		[]byte(strings.Repeat("u", 200)),
		[]byte(strings.Repeat("v", 100)),
		[]byte(strings.Repeat("w", 100)),
		[]byte(strings.Repeat("x", 200)),
		[]byte(strings.Repeat("y", 400)),
		[]byte(strings.Repeat("z", 200)),
		[]byte(strings.Repeat("a", 200)),
		[]byte(strings.Repeat("b", 150)),
		[]byte(strings.Repeat("c", 200)),
		[]byte(strings.Repeat("d", 100)),
		[]byte(strings.Repeat("e", 300)),
		[]byte(strings.Repeat("f", 100)),
		[]byte(strings.Repeat("g", 300)),
		[]byte(strings.Repeat("h", 200)),
		[]byte(strings.Repeat("i", 100)),
		[]byte(strings.Repeat("j", 100)),
		[]byte(strings.Repeat("k", 200)),
		[]byte(strings.Repeat("l", 100)),
		[]byte(strings.Repeat("m", 200)),
		[]byte(strings.Repeat("n", 160)),
		[]byte(strings.Repeat("o", 220)),
		[]byte(strings.Repeat("p", 400)),
		[]byte(strings.Repeat("q", 100)),
		[]byte(strings.Repeat("r", 200)),
		[]byte(strings.Repeat("s", 400)),
		[]byte(strings.Repeat("t", 100)),
		[]byte(strings.Repeat("u", 200)),
		[]byte(strings.Repeat("v", 100)),
		[]byte(strings.Repeat("w", 100)),
		[]byte(strings.Repeat("x", 200)),
		[]byte(strings.Repeat("y", 400)),
		[]byte(strings.Repeat("z", 200)),
	}
	codecErr := make([]error, len(codecData))

	client.encoder = &StubCodec{dat: codecData, err: codecErr}

	putRecordsOutputGood := &kinesis.PutRecordsOutput{
		Records:           []*kinesis.PutRecordsResultEntry{{ErrorCode: aws.String("")}},
		FailedRecordCount: aws.Int64(0),
	}

	putRecordsOut := []*kinesis.PutRecordsOutput{
		putRecordsOutputGood,
		putRecordsOutputGood,
	}
	putRecordsErr := make([]error, len(putRecordsOut))
	kinesisStub := &StubClient{out: putRecordsOut, err: putRecordsErr}
	client.streams = kinesisStub

	for _, _ = range codecData {
		events = append(events, publisher.Event{
			Content: beat.Event{
				Fields: common.MapStr{
					fieldForPartitionKey: "expectedPartitionKey",
				},
			},
		},
		)
	}
	rest, err := client.publishEvents(events)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(rest) != 0 {
		t.Errorf("unexpected number of remaining events: %d", len(rest))
	}
	if len(kinesisStub.calls) != 2 {
		t.Errorf("unexpected number of batches: %d", len(kinesisStub.calls))
	}
	if len(kinesisStub.calls[0].Records) != 15 {
		t.Errorf("unexpected number of records in batch 0 got: %d", len(kinesisStub.calls[0].Records))
	}
	if len(kinesisStub.calls[1].Records) != 13 {
		t.Errorf("unexpected number of records in batch 1 got: %d", len(kinesisStub.calls[1].Records))
	}
	// var dst []byte
	// base64.Decode(dst, kinesisStub.calls[0].Records[0].Data)
	MAX_RECORD_SIZE = origMaxSizeOfRecord // cleanup
}

func TestClient_String(t *testing.T) {
	fieldForPartitionKey := "mypartitionkey"
	provider := newFieldPartitionKeyProvider(fieldForPartitionKey)
	codecData := [][]byte{[]byte("boom")}
	codecErr := []error{nil}
	client := client{encoder: &StubCodec{dat: codecData, err: codecErr}, partitionKeyProvider: provider}

	if v := client.String(); v != "streams" {
		t.Errorf("unexpected value '%v'", v)
	}
}

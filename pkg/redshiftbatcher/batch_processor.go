package redshiftbatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	multierror "github.com/hashicorp/go-multierror"
	"github.com/practo/klog/v2"
	"github.com/practo/tipoca-stream/pkg/kafka"
	"github.com/practo/tipoca-stream/pkg/redshift"
	loader "github.com/practo/tipoca-stream/pkg/redshiftloader"
	"github.com/practo/tipoca-stream/pkg/s3sink"
	"github.com/practo/tipoca-stream/pkg/schemaregistry"
	"github.com/practo/tipoca-stream/pkg/serializer"
	"github.com/practo/tipoca-stream/pkg/transformer"
	"github.com/practo/tipoca-stream/pkg/transformer/debezium"
	"github.com/practo/tipoca-stream/pkg/transformer/masker"
	"github.com/practo/tipoca-stream/pkg/util"
	"github.com/spf13/viper"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

type batchProcessor struct {
	topic             string
	partition         int32
	loaderTopicPrefix string
	consumerGroupID   string
	autoCommit        bool

	s3sink      *s3sink.S3Sink
	s3BucketDir string

	// messageTransformer is used to transform debezium events into
	// redshift COPY commands with some annotations
	messageTransformer transformer.MessageTransformer
	// schemaTransfomer is used to transform debezium schema
	// to redshift table
	schemaTransformer transformer.SchemaTransformer
	// msgMasker is used to mask the message based on the configuration
	// provided. Default is always to mask everything.
	// this gets activated only when batcher.mask is set to true
	// and batcher.maskConfigDir is defined.
	msgMasker transformer.MessageTransformer
	// maskMessages stores if the masking is enabled
	maskMessages bool

	// signaler is a kafka producer signaling the load the batch uploaded data
	// TODO: make the producer have interface
	signaler *kafka.AvroProducer

	maxConcurrency int

	// loaderSchemaID stores the schema ID for the loader topic-value
	loaderSchemaID int

	// schemaIDKey stores the schema ID for the batcher topic-key
	// loader would use these to fetch primaryKeys for the table
	schemaIDKey int

	// metricSetter sets the batch metrics
	metric metricSetter
}

func newBatchProcessor(
	consumerGroupID string,
	topic string,
	partition int32,
	processChan chan []*serializer.Message,
	kafkaConfig kafka.KafkaConfig,
	saramaConfig kafka.SaramaConfig,
	maskConfig masker.MaskConfig,
	kafkaLoaderTopicPrefix string,
	maxConcurrency int,
) (
	serializer.MessageBatchAsyncProcessor,
	error,
) {
	sink, err := s3sink.NewS3Sink(
		viper.GetString("s3sink.accessKeyId"),
		viper.GetString("s3sink.secretAccessKey"),
		viper.GetString("s3sink.region"),
		viper.GetString("s3sink.bucket"),
	)
	if err != nil {
		return nil, fmt.Errorf("Error creating s3 client: %v\n", err)
	}

	signaler, err := kafka.NewAvroProducer(
		strings.Split(kafkaConfig.Brokers, ","),
		kafkaConfig.Version,
		kafkaConfig.TLSConfig,
		kafkaConfig.SaslConfig,
	)
	if err != nil {
		return nil, fmt.Errorf("unable to make signaler client, err:%v\n", err)
	}

	var msgMasker transformer.MessageTransformer
	maskMessages := viper.GetBool("batcher.mask")
	if maskMessages {
		msgMasker = masker.NewMsgMasker(
			viper.GetString("batcher.maskSalt"),
			topic,
			maskConfig,
		)
	}

	registry := schemaregistry.NewRegistry(viper.GetString("schemaRegistryURL"))
	// creates the loader schema for value if not present
	loaderSchemaID, _, err := schemaregistry.CreateSchema(
		registry,
		kafkaLoaderTopicPrefix+topic,
		loader.JobAvroSchema,
		false, // key is false means its for the value
	)
	if err != nil {
		return nil, fmt.Errorf(
			"Error creating schema for topic: %s, err: %v",
			kafkaLoaderTopicPrefix+topic, err)
	}
	schemaKey, err := schemaregistry.GetLatestSchemaWithRetry(
		registry,
		topic,
		true, // key is true means its for the key
		2,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"Error fetching schema for topic-key for topic: %s, err: %v",
			topic, err)
	}
	if schemaKey == nil {
		return nil, fmt.Errorf(
			"Error since schema came as nil for topic-key for topic: %s",
			topic,
		)
	}

	klog.V(2).Infof("%s: autoCommit: %v", topic, saramaConfig.AutoCommit)

	return &batchProcessor{
		topic:              topic,
		partition:          partition,
		loaderTopicPrefix:  kafkaLoaderTopicPrefix,
		consumerGroupID:    consumerGroupID,
		autoCommit:         saramaConfig.AutoCommit,
		s3sink:             sink,
		s3BucketDir:        viper.GetString("s3sink.bucketDir"),
		messageTransformer: debezium.NewMessageTransformer(),
		schemaTransformer: debezium.NewSchemaTransformer(
			viper.GetString("schemaRegistryURL")),
		msgMasker:      msgMasker,
		maskMessages:   maskMessages,
		signaler:       signaler,
		maxConcurrency: maxConcurrency,
		loaderSchemaID: loaderSchemaID,
		schemaIDKey:    schemaKey.ID(),
		metric: metricSetter{
			consumergroup: consumerGroupID,
			topic:         topic,
			sinkGroup:     viper.GetString("sinkGroup"),
		},
	}, nil
}

type response struct {
	err               error
	batchID           int
	batchSchemaID     int
	batchSchemaTable  redshift.Table
	skipMerge         bool
	createEvents      int64
	updateEvents      int64
	deleteEvents      int64
	s3Key             string
	bodyBuf           *bytes.Buffer
	startOffset       int64
	endOffset         int64
	messagesProcessed int
	maskSchema        map[string]serializer.MaskInfo
	extraMaskSchema   map[string]serializer.ExtraMaskInfo
	bytesProcessed    int64
}

func (b *batchProcessor) ctxCancelled(ctx context.Context) error {
	select {
	case <-ctx.Done():
		klog.Warningf(
			"%s, session ctx done. Cancelled.\n",
			b.topic,
		)
		return kafka.ErrSaramaSessionContextDone
	default:
		return nil
	}
}

func constructS3key(
	s3ucketDir string,
	consumerGroupID string,
	topic string,
	partition int32,
	offset int64,
) string {
	s3FileName := fmt.Sprintf(
		"%d_offset_%d_partition.json.gz",
		offset,
		partition,
	)

	maskFileVersion := viper.GetString("batcher.maskFileVersion")
	if maskFileVersion != "" {
		return filepath.Join(
			s3ucketDir,
			consumerGroupID,
			topic,
			maskFileVersion,
			s3FileName,
		)
	} else {
		return filepath.Join(
			s3ucketDir,
			consumerGroupID,
			topic,
			s3FileName,
		)
	}
}

func (b *batchProcessor) handleShutdown() {
	klog.V(2).Infof("%s: batch processing gracefully shutingdown", b.topic)
	b.signaler.Close()
}

func (b *batchProcessor) markOffset(
	session sarama.ConsumerGroupSession,
	topic string,
	partition int32,
	offset int64,
	autoCommit bool,
) {
	klog.V(2).Infof("%s: offset: %v, marking", topic, offset+1)
	session.MarkOffset(
		topic,
		partition,
		offset+1,
		"",
	)
	klog.V(2).Infof("%s: offset: %v, marked", topic, offset+1)

	if autoCommit == false {
		klog.V(2).Infof("%s: committing (autoCommit=false)", topic)
		session.Commit()
		klog.V(2).Infof("%s: committed (autoCommit=false)", topic)
	}
}

func (b *batchProcessor) signalLoad(resp *response) error {
	klog.V(4).Infof("%s: batchID:%d: signalling", b.topic, resp.batchID)

	job := loader.NewJob(
		b.topic,
		resp.startOffset,
		resp.endOffset,
		",",
		b.s3sink.GetKeyURI(resp.s3Key),
		resp.batchSchemaID, // schema of upstream topic's value
		b.schemaIDKey,      // schema of upstream topic's key
		resp.maskSchema,
		resp.extraMaskSchema,
		resp.skipMerge,
		resp.bytesProcessed,
		resp.createEvents,
		resp.updateEvents,
		resp.deleteEvents,
	)

	err := b.signaler.Add(
		b.loaderTopicPrefix+b.topic,
		loader.JobAvroSchema,
		b.loaderSchemaID,
		[]byte(time.Now().String()),
		job.ToStringMap(),
	)
	if err != nil {
		return err
	}
	klog.V(2).Infof(
		"%s: batchID:%d: signalled loader.\n",
		b.topic, resp.batchID,
	)

	return nil
}

func removeEmptyNullValues(value map[string]*string) map[string]*string {
	for cName, cVal := range value {
		if cVal == nil {
			delete(value, cName)
			continue
		}

		if strings.TrimSpace(*cVal) == "" {
			delete(value, cName)
			continue
		}
	}

	return value
}

func (b *batchProcessor) processMessage(
	ctx context.Context,
	message *serializer.Message,
	resp *response,
	messageID int,
) (int64, error) {
	var bytesProcessed int64

	klog.V(5).Infof(
		"%s: batchID:%d id:%d: transforming",
		b.topic, resp.batchID, messageID,
	)

	// key is always made based on the first not nil message in the batch
	// also the batchSchemaId is set only at the start of the batch
	if resp.s3Key == "" {
		resp.batchSchemaID = message.SchemaId
		r, err := b.schemaTransformer.TransformValue(
			b.topic,
			resp.batchSchemaID,
			b.schemaIDKey,
			resp.maskSchema,
			resp.extraMaskSchema,
		)
		if err != nil {
			return bytesProcessed, fmt.Errorf(
				"transforming schema:%d => inputTable failed: %v",
				resp.batchSchemaID,
				err,
			)
		}
		resp.batchSchemaTable = r.(redshift.Table)
		resp.s3Key = constructS3key(
			b.s3BucketDir,
			b.consumerGroupID,
			message.Topic,
			message.Partition,
			message.Offset,
		)
		resp.startOffset = message.Offset
	}

	if resp.batchSchemaID != message.SchemaId {
		return bytesProcessed, fmt.Errorf("%s: schema id mismatch in the batch, %d != %d",
			b.topic,
			resp.batchSchemaID,
			message.SchemaId,
		)
	}

	err := b.messageTransformer.Transform(message, resp.batchSchemaTable)
	if err != nil {
		return bytesProcessed, fmt.Errorf(
			"Error transforming message:%+v, err:%v", message, err,
		)
	}

	if b.maskMessages {
		err := b.msgMasker.Transform(message, resp.batchSchemaTable)
		if err != nil {
			return bytesProcessed, fmt.Errorf(
				"Error masking message:%+v, err:%v", message, err)
		}
	}

	message.Value = removeEmptyNullValues(message.Value.(map[string]*string))
	messageValueBytes, err := json.Marshal(message.Value)
	if err != nil {
		return bytesProcessed, fmt.Errorf(
			"Error marshalling message.Value, message: %+v", message)
	}

	resp.bodyBuf.Write(messageValueBytes)
	resp.bodyBuf.Write([]byte{'\n'})

	bytesProcessed += message.Bytes

	if b.maskMessages && len(resp.maskSchema) == 0 {
		resp.maskSchema = message.MaskSchema
	}
	if b.maskMessages && len(resp.extraMaskSchema) == 0 {
		resp.extraMaskSchema = message.ExtraMaskSchema
	}

	resp.skipMerge = false // deprecated
	klog.V(5).Infof(
		"%s: batchID:%d id:%d: transformed\n",
		b.topic, resp.batchID, messageID,
	)
	resp.endOffset = message.Offset

	return bytesProcessed, nil
}

// processMessages handles the batch procesing and return true if all completes
// otherwise return false in case of gracefull shutdown signals being captured,
// this helps in cleanly shutting down the batch processing.
func (b *batchProcessor) processMessages(
	ctx context.Context,
	msgBuf []*serializer.Message,
	resp *response,
) (int64, error) {
	var totalBytesProcessed int64
	for messageID, message := range msgBuf {
		select {
		case <-ctx.Done():
			return totalBytesProcessed, kafka.ErrSaramaSessionContextDone
		default:
			bytesProcessed, err := b.processMessage(ctx, message, resp, messageID)
			if err != nil {
				return totalBytesProcessed, err
			}
			totalBytesProcessed += bytesProcessed

			switch message.Operation {
			case serializer.OperationCreate:
				resp.createEvents += 1
			case serializer.OperationUpdate:
				resp.updateEvents += 1
			case serializer.OperationDelete:
				resp.deleteEvents += 1
			default:
				klog.Fatalf("Unkown operation: %+v, message: %+v", message.Operation, message)
			}
		}
	}

	return totalBytesProcessed, nil
}

func (b *batchProcessor) processBatch(
	wg *sync.WaitGroup,
	session sarama.ConsumerGroupSession,
	msgBuf []*serializer.Message,
	resp *response,
) {
	defer wg.Done()

	ctx := session.Context()
	err := b.ctxCancelled(ctx)
	if err != nil {
		resp.err = err
		return
	}

	klog.V(4).Infof("%s: batchID:%d, size:%d: processing...",
		b.topic, resp.batchID, len(msgBuf),
	)
	resp.bytesProcessed, err = b.processMessages(ctx, msgBuf, resp)
	if err != nil {
		resp.err = err
		return
	}

	// Upload
	klog.V(4).Infof("%s: batchId:%d, size:%d: uploading...",
		b.topic, resp.batchID, len(msgBuf),
	)

	// Compress
	gzBodyBuf := bytes.NewBuffer(make([]byte, 0, 4096))
	err = util.GzipWrite(gzBodyBuf, resp.bodyBuf.Bytes())
	if err != nil {
		klog.Fatalf("Error in compressing, exitting, err:%v", err)
	}

	err = b.s3sink.Upload(resp.s3Key, gzBodyBuf)
	if err != nil {
		resp.err = fmt.Errorf("Error writing to s3, err=%v", err)
		return
	}
	klog.V(2).Infof(
		"%s: batchID:%d, startOffset:%d, endOffset:%d: uploaded",
		b.topic, resp.batchID, resp.startOffset, resp.endOffset,
	)
	klog.V(2).Infof("%s: bytes: %v, s3Key: %v", b.topic, resp.bytesProcessed, resp.s3Key)
	resp.bodyBuf.Truncate(0)
	gzBodyBuf.Truncate(0)
	resp.messagesProcessed = len(msgBuf)
}

// Process implements serializer.MessageBatchAsyncProcessor
func (b *batchProcessor) Process(
	pwg *sync.WaitGroup,
	session sarama.ConsumerGroupSession,
	processChan <-chan []*serializer.Message,
	errChan chan<- error,
) {
	defer pwg.Done()

	timeoutTicker := time.NewTicker(10 * time.Second)
	klog.V(4).Infof("%s: processor started", b.topic)

	for {
		breakLoop := false
		msgBufs := [][]*serializer.Message{}

		klog.V(2).Infof(
			"%s: processChan:%v",
			b.topic,
			len(processChan),
		)
		// read multiple msgs from buffer for concurrent batches
		for {
			// using label break is less readable
			if breakLoop {
				break
			}
			select {
			case <-session.Context().Done():
				klog.V(2).Infof(
					"%s: processor returning, session ctx done",
					b.topic,
				)
				return
			case msgBuf := <-processChan:
				msgBufs = append(msgBufs, msgBuf)
				if len(msgBufs) == b.maxConcurrency {
					breakLoop = true
					break
				}
			case <-timeoutTicker.C:
				if len(msgBufs) > 0 {
					breakLoop = true
					break
				}
			}
		}

		// trigger concurrent batches
		klog.V(2).Infof("%s: processing...", b.topic)
		wg := &sync.WaitGroup{}
		responses := []*response{}
		for i, msgBuf := range msgBufs {
			resp := &response{
				err:             nil,
				batchID:         i + 1,
				batchSchemaID:   -1,
				skipMerge:       false,
				createEvents:    0,
				updateEvents:    0,
				deleteEvents:    0,
				s3Key:           "",
				bodyBuf:         bytes.NewBuffer(make([]byte, 0, 4096)),
				maskSchema:      make(map[string]serializer.MaskInfo),
				extraMaskSchema: make(map[string]serializer.ExtraMaskInfo),
			}
			wg.Add(1)
			go b.processBatch(wg, session, msgBuf, resp)
			responses = append(responses, resp)
		}
		if len(responses) == 0 {
			klog.Fatalf("%s: no batch to process (unexpected)", b.topic)
		}
		klog.V(2).Infof("%s: waiting to finish (%d batches)...", b.topic, len(responses))
		wg.Wait()
		klog.V(2).Infof("%s: finished (%d batches)", b.topic, len(responses))

		// return if there was any error in processing any of the batches
		var totalBytesProcessed int64 = 0
		totalMessagesProcessed := 0
		var errors error
		for _, resp := range responses {
			totalBytesProcessed += resp.bytesProcessed
			totalMessagesProcessed += resp.messagesProcessed
			if resp.err != nil {
				if resp.err == kafka.ErrSaramaSessionContextDone {
					klog.V(2).Infof(
						"%s: processor returning, session ctx done",
						b.topic,
					)
					return
				}
				errors = multierror.Append(errors, resp.err)
			}
		}
		if errors != nil {
			klog.Errorf(
				"%s, error(s) occured in processing (sending err)", b.topic,
			)
			b.handleShutdown()

			// send to channel with context check, fix #170
			select {
			case <-session.Context().Done():
				klog.V(2).Infof(
					"%s: processor returning, session ctx done",
					b.topic,
				)
				return
			case errChan <- errors:
			}

			klog.Errorf(
				"%s, error(s) occured: %+v, processor shutdown.",
				b.topic,
				errors,
			)
			return
		}

		// signal load for all the processed messages
		// failure in between signal and marking the offset can lead to
		// duplicates in the loader topic, but it's ok as loader is idempotent
		for _, resp := range responses {
			select {
			default:
			case <-session.Context().Done():
				klog.V(2).Infof(
					"%s: processor returning, session ctx done",
					b.topic,
				)
				return
			}
			err := b.signalLoad(resp)
			if err != nil {
				// send to channel with context check, fix #170
				select {
				case <-session.Context().Done():
					klog.V(2).Infof(
						"%s: processor returning, session ctx done",
						b.topic,
					)
					return
				case errChan <- err:
				}
				klog.Errorf(
					"%s, error signalling: %v, processor shutdown.",
					b.topic,
					err,
				)
				b.handleShutdown()
				return
			}
		}

		// mark the last offset of the last batch
		first := responses[0]
		last := responses[len(responses)-1]
		b.markOffset(session, b.topic, 0, last.endOffset, b.autoCommit)

		// set cumulative metrics
		b.metric.setBytesProcessed(totalBytesProcessed)
		b.metric.setMsgsProcessed(totalMessagesProcessed)

		klog.V(2).Infof(
			"%s: startOffset:%d, endOffset:%d, processed",
			b.topic,
			first.startOffset,
			last.endOffset,
		)
	}
}

package s3sink

import (
	"bytes"
	"fmt"

	"encoding/json"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// S3Sink is a library which can be used to upload data to s3
type S3Sink struct {
	// uploader client from aws which makes the API call to aws for upload
	uploader *s3manager.Uploader

	// bucket is the s3 bucket name to store data
	bucket string
}

type S3Manifest struct {
	Entries []S3ManifestEntry `json:"entries"`
}

type S3ManifestEntry struct {
	URL       string `json:"url"`
	Mandatory bool   `json:"mandatory"`
}

type Config struct {
	Region          string `yaml:"region"`
	AccessKeyId     string `yaml:"accessKeyId"`
	SecretAccessKey string `yaml:"secretAccessKey"`
	Bucket          string `yaml:"bucket"`
	BucketDir       string `yaml:"bucketDir"`
}

// NewS3Sink is the factory method constructing a new S3Sink
func NewS3Sink(
	awsAccessKeyID string,
	awsSecretAccessKey string,
	s3Region string,
	s3Bucket string) (*S3Sink, error) {

	awsConfig := &aws.Config{
		Region: aws.String(s3Region),
	}

	awsConfig = awsConfig.WithCredentialsChainVerboseErrors(true)
	sess, err := session.NewSession(awsConfig)
	if err != nil {
		return nil, err
	}

	uploader := s3manager.NewUploader(sess)

	s := &S3Sink{
		uploader: uploader,
		bucket:   s3Bucket,
	}

	return s, nil
}

func (s *S3Sink) GetKeyURI(key string) string {
	return fmt.Sprintf(
		"s3://%s/%s",
		s.bucket,
		key,
	)
}

// Upload uploads the data stored in buffer to s3 in the specified key
// and clears the buffer
func (s *S3Sink) Upload(key string, bodyBuf *bytes.Buffer) error {
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
		Body:   bodyBuf,
	})
	if err != nil {
		return err
	}

	return nil
}

func (s *S3Sink) UploadS3Manifest(key string, entries []S3ManifestEntry) error {
	s3Manifest := S3Manifest{
		Entries: entries,
	}
	s3Bytes, err := json.Marshal(s3Manifest)
	if err != nil {
		return err
	}
	bodyBuf := bytes.NewBuffer(make([]byte, 0, 4096))
	bodyBuf.Write(s3Bytes)

	return s.Upload(key, bodyBuf)
}

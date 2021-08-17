package s3

import (
	"bytes"
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
	"strings"
)

type S3DaoClntI interface {
	GetTextFromS3(s3BucketName, s3Key string) (string, error)
	StoreTextInS3(s3BucketName, s3Key, payloadContentStr string) error
	DeletePayloadFromS3(s3BucketName, s3Key string) error
}

type S3Dao struct {
	// private static final Logger LOG = LoggerFactory.getLogger(S3Dao.class);
	S3Client             S3SvcClntI
	ServerSideEncryption types.ServerSideEncryption
	ObjectCannedACL      types.ObjectCannedACL
}

func (dao *S3Dao) GetTextFromS3(s3BucketName, s3Key string) (string, error) {
	getObjectInput := &s3.GetObjectInput{
		Bucket: &s3BucketName,
		Key:    &s3Key,
	}

	ctx := context.Background()
	object, err := dao.S3Client.GetObject(ctx, getObjectInput)
	if err != nil {
		err := errors.New("Failed to get the S3Client object which contains the payload.")
		log.Println(err)
		return "", err
	}

	buf := new(bytes.Buffer)
	_, err = buf.ReadFrom(object.Body)
	if err != nil {
		err := errors.New("Failure when handling the message which was read from S3Client object.")
		log.Println(err)
		return "", err
	}
	embeddedText := buf.String()

	return embeddedText, nil
}

func (dao *S3Dao) StoreTextInS3(s3BucketName, s3Key, payloadContentStr string) error {
	payloadReader := strings.NewReader(payloadContentStr)
	putObjectInput := &s3.PutObjectInput{
		Bucket: &s3BucketName,
		Key:    &s3Key,
		Body:   payloadReader,
	}
	if &dao.ObjectCannedACL != nil {
		putObjectInput.ACL = dao.ObjectCannedACL
	}

	if dao.ServerSideEncryption != "" {
		putObjectInput.ServerSideEncryption = dao.ServerSideEncryption
	}

	ctx := context.Background()
	if dao.ServerSideEncryption != "" {
		defEnc := &types.ServerSideEncryptionByDefault{
			SSEAlgorithm:   dao.ServerSideEncryption,
			KMSMasterKeyID: &s3Key,
		}
		rule := types.ServerSideEncryptionRule{ApplyServerSideEncryptionByDefault: defEnc}
		rules := []types.ServerSideEncryptionRule{rule}
		encrConfig := &types.ServerSideEncryptionConfiguration{Rules: rules}
		encryptionInput := &s3.PutBucketEncryptionInput{Bucket: &s3BucketName, ServerSideEncryptionConfiguration: encrConfig}
		dao.S3Client.PutBucketEncryption(ctx, encryptionInput)
	}

	_, err := dao.S3Client.PutObject(ctx, putObjectInput)
	if err != nil {
		log.Println(err)
		return errors.New("Failed to store the message content in an S3Client object.")
	}

	return nil
}

func (dao *S3Dao) DeletePayloadFromS3(s3BucketName, s3Key string) error {
	deleteObjectInput := &s3.DeleteObjectInput{
		Bucket: &s3BucketName,
		Key:    &s3Key,
	}
	ctx := context.Background()
	_, err := dao.S3Client.DeleteObject(ctx, deleteObjectInput)
	if err != nil {
		err := errors.New("Failed to delete the S3Client object which contains the payload")
		log.Println(err)
		return err
	}
	log.Printf("S3Client object deleted, Bucket name: %s, Object key:  %s .", s3BucketName, s3Key) // info

	return nil
}

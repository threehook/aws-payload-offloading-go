package s3

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/threehook/aws-payload-offloading-go/mocks"
	"strings"
	"testing"
)

var (
	s3BucketName                 = "test-bucket-name"
	anyPayload                   = "AnyPayload"
	anyS3Key                     = "AnyS3key"
	serverSideEncryptionStrategy = types.ServerSideEncryptionAwsKms
	objectCannedACL              = types.ObjectCannedACLPublicRead
)

func TestStoreTextInS3WithoutSSEOrCanned(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Client := mocks.NewMockS3SvcClntI(mockCtrl)

	// Expect PutObject to be called once with context and PutObjectInput as parameters. Ignore the Return.
	ctx := context.Background()
	input := s3.PutObjectInput{Bucket: &s3BucketName, Key: &anyS3Key, Body: strings.NewReader(anyPayload)}

	var capturedArgsMap = make(map[string]interface{})
	mockS3Client.EXPECT().PutObject(ctx, &input).Do(
		func(ctx context.Context, input *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["serverSideEncryption"] = input.ServerSideEncryption
			capturedArgsMap["acl"] = input.ACL
			capturedArgsMap["bucket"] = *input.Bucket
		},
	).Times(1)

	dao := S3Dao{S3Client: mockS3Client}
	err := dao.StoreTextInS3(s3BucketName, anyS3Key, anyPayload)
	if err != nil {
		t.Errorf("Expected no error, but got: '%v'", err)
	}

	assert := assert.New(t)
	assert.Empty(capturedArgsMap["serverSideEncryption"])
	assert.Empty(capturedArgsMap["acl"])
	assert.Equal(capturedArgsMap["bucket"], s3BucketName)
}

func TestStoreTextInS3WithSSE(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Client := mocks.NewMockS3SvcClntI(mockCtrl)

	// Expect PutObject to be called once with context and PutObjectInput as parameters. Ignore the Return.
	ctx := context.Background()
	input := s3.PutObjectInput{Bucket: &s3BucketName, Key: &anyS3Key, Body: strings.NewReader(anyPayload), ServerSideEncryption: serverSideEncryptionStrategy}

	var capturedArgsMap = make(map[string]interface{})
	mockS3Client.EXPECT().PutObject(ctx, &input).Do(
		func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["serverSideEncryption"] = input.ServerSideEncryption
			capturedArgsMap["acl"] = input.ACL
			capturedArgsMap["bucket"] = *input.Bucket
		},
	).Times(1)

	mockS3Client.EXPECT().PutBucketEncryption(ctx, gomock.Any()).Times(1)

	dao := S3Dao{S3Client: mockS3Client, ServerSideEncryption: serverSideEncryptionStrategy, ObjectCannedACL: ""}
	err := dao.StoreTextInS3(s3BucketName, anyS3Key, anyPayload)
	if err != nil {
		t.Errorf("Expected no error, but got: '%v'", err)
	}

	assert := assert.New(t)
	assert.Equal(capturedArgsMap["serverSideEncryption"], serverSideEncryptionStrategy)
	assert.Empty(capturedArgsMap["acl"])
	assert.Equal(capturedArgsMap["bucket"], s3BucketName)
}

func TestStoreTextInS3WithBoth(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Client := mocks.NewMockS3SvcClntI(mockCtrl)

	// Expect PutObject to be called once with context and PutObjectInput as parameters. Ignore the Return.
	ctx := context.Background()
	input := s3.PutObjectInput{Bucket: &s3BucketName, Key: &anyS3Key, Body: strings.NewReader(anyPayload), ServerSideEncryption: serverSideEncryptionStrategy, ACL: objectCannedACL}

	var capturedArgsMap = make(map[string]interface{})
	mockS3Client.EXPECT().PutObject(ctx, &input).Do(
		func(ctx context.Context, params *s3.PutObjectInput, optFns ...func(options *s3.Options)) {
			capturedArgsMap["serverSideEncryption"] = input.ServerSideEncryption
			capturedArgsMap["acl"] = input.ACL
			capturedArgsMap["bucket"] = *input.Bucket
		},
	).Times(1)

	mockS3Client.EXPECT().PutBucketEncryption(ctx, gomock.Any()).Times(1)

	dao := S3Dao{S3Client: mockS3Client, ServerSideEncryption: serverSideEncryptionStrategy, ObjectCannedACL: objectCannedACL}
	err := dao.StoreTextInS3(s3BucketName, anyS3Key, anyPayload)
	if err != nil {
		t.Errorf("Expected no error, but got: '%v'", err)
	}

	assert := assert.New(t)
	assert.Equal(capturedArgsMap["serverSideEncryption"], serverSideEncryptionStrategy)
	assert.Equal(capturedArgsMap["acl"], objectCannedACL)
	assert.Equal(capturedArgsMap["bucket"], s3BucketName)
}

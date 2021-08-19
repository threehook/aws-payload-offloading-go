package encryption

import (
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

type EncryptionStrategy interface {
	Decorate(input *s3.PutObjectInput)
}

type CustomerKey struct {
	AwsKmsKeyId string
}

type AwsManagedCmk struct{}

func (c *CustomerKey) Decorate(input *s3.PutObjectInput) {
	input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
	input.SSEKMSKeyId = &c.AwsKmsKeyId
}

func (a *AwsManagedCmk) Decorate(input *s3.PutObjectInput) {
	input.ServerSideEncryption = types.ServerSideEncryptionAwsKms
}

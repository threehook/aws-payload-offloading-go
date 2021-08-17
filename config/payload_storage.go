package config

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/threehook/aws-payload-offloading-go/s3"
	"log"
)

type PayloadStorageConfig struct {
	//private static final Logger LOG = LoggerFactory.getLogger(PayloadStorageConfig.class);
	S3Client             s3.S3SvcClntI
	S3BucketName         string
	PayloadSizeThreshold int
	AlwaysThroughS3      bool
	PayloadSupport       bool
	// This field is optional, it is set only when we want to configure S3 Server Side Encryption with KMS.
	ServerSideEncryption types.ServerSideEncryption
	// This field is optional, it is set only when we want to add access control list to Amazon S3 buckets and objects
	ObjectCannedACL types.ObjectCannedACL
}

func NewPayloadStorageConfigurationFromOther(other *PayloadStorageConfig) *PayloadStorageConfig {
	return &PayloadStorageConfig{
		S3Client:             other.S3Client,
		S3BucketName:         other.S3BucketName,
		PayloadSupport:       other.PayloadSupport,
		AlwaysThroughS3:      other.AlwaysThroughS3,
		PayloadSizeThreshold: other.PayloadSizeThreshold,
		ServerSideEncryption: other.ServerSideEncryption,
		ObjectCannedACL:      other.ObjectCannedACL,
	}
}

// SetPayloadSupportEnabled enables support for payloads
func (psc *PayloadStorageConfig) SetPayloadSupportEnabled(s3Client s3.S3SvcClntI, s3BucketName string) error {
	if &s3Client == nil || &s3BucketName == nil {
		err := errors.New("S3Client client and/or S3Client bucket name cannot be null.")
		log.Println(err)
		return err
	}
	if psc.PayloadSupport {
		log.Println("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName.") // warn
	}
	psc.S3Client = s3Client
	psc.S3BucketName = s3BucketName
	psc.PayloadSupport = true
	log.Println("Payload support enabled.") // info

	return nil
}

// WithPayloadSupportEnabled enables support for payload
func (psc *PayloadStorageConfig) WithPayloadSupportEnabled(s3Client s3.S3SvcClntI, s3BucketName string) (*PayloadStorageConfig, error) {
	if err := psc.SetPayloadSupportEnabled(s3Client, s3BucketName); err != nil {
		return nil, err
	}
	return psc, nil
}

// SetPayloadSupportDisabled disables support for payloads
func (psc *PayloadStorageConfig) SetPayloadSupportDisabled() {
	psc.S3Client = nil
	psc.S3BucketName = ""
	psc.PayloadSupport = false
	log.Println("Payload support disabled.") // info
}

package payload_offloading

import (
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"log"
)

type PayloadStorageConfiguration struct {

	//private static final Logger LOG = LoggerFactory.getLogger(PayloadStorageConfiguration.class);

	S3                   *s3.Client
	S3BucketName         string
	PayloadSizeThreshold int
	AlwaysThroughS3      bool
	PayloadSupport       bool
	 // This field is optional, it is set only when we want to configure S3 Server Side Encryption with KMS.
	ServerSideEncryptionStrategy types.ServerSideEncryption
	 // This field is optional, it is set only when we want to add access control list to Amazon S3 buckets and objects
	ObjectCannedACL types.ObjectCannedACL
}

func NewPayloadStorageConfiguration() *PayloadStorageConfiguration {
	return &PayloadStorageConfiguration{}
}

func NewPayloadStorageConfigurationFromOther(other *PayloadStorageConfiguration) *PayloadStorageConfiguration {
	return &PayloadStorageConfiguration{
		S3:                           other.S3,
		S3BucketName:                 other.S3BucketName,
		PayloadSupport:               other.PayloadSupport,
		AlwaysThroughS3:              other.AlwaysThroughS3,
		PayloadSizeThreshold:         other.PayloadSizeThreshold,
		ServerSideEncryptionStrategy: other.ServerSideEncryptionStrategy,
		ObjectCannedACL:              other.ObjectCannedACL,
	}
}

// SetPayloadSupportEnabled enables support for payloads
func (psc *PayloadStorageConfiguration) SetPayloadSupportEnabled(s3 *s3.Client, s3BucketName string) error {
	if &s3 == nil || &s3BucketName == nil {
		err := errors.New("S3 client and/or S3 bucket name cannot be null.")
		log.Println(err)
		return err
	}
	if psc.PayloadSupport {
		log.Println("Payload support is already enabled. Overwriting AmazonS3Client and S3BucketName."); // warn
	}
	psc.S3 = s3
	psc.S3BucketName = s3BucketName
	psc.PayloadSupport = true
	log.Println("Payload support enabled.") // info

	return nil
}

// WithPayloadSupportEnabled enables support for payload
func (psc *PayloadStorageConfiguration) WithPayloadSupportEnabled(s3 *s3.Client, s3BucketName string) (*PayloadStorageConfiguration, error) {
	if err := psc.SetPayloadSupportEnabled(s3, s3BucketName); err != nil {
		return nil, err
	}
	return psc, nil
}

// SetPayloadSupportDisabled disables support for payloads
func (psc *PayloadStorageConfiguration) SetPayloadSupportDisabled() {
	psc.S3 = nil
	psc.S3BucketName = ""
	psc.PayloadSupport = false
	log.Println("Payload support disabled.") // info

}

//// WithPayloadSupportDisabled disables support for payload
//func (psc *PayloadStorageConfiguration) WithPayloadSupportDisabled() *PayloadStorageConfiguration {
//	psc.SetPayloadSupportDisabled()
//	return psc
//}
//
//// WithPayloadSizeThreshold sets the payload size threshold for storing payloads in Amazon S3
//func (psc *PayloadStorageConfiguration) WithPayloadSizeThreshold(payloadSizeThreshold int) *PayloadStorageConfiguration {
//	psc.PayloadSizeThreshold = payloadSizeThreshold
//	return psc
//}
//
//// WithAlwaysThroughS3 sets whether or not all payloads regardless of their size should be stored in Amazon S3
//func (psc *PayloadStorageConfiguration) WithAlwaysThroughS3(alwaysThroughS3 bool) *PayloadStorageConfiguration {
//	psc.AlwaysThroughS3 = alwaysThroughS3
//	return psc
//}
//
//// WithServerSideEncryption sets which method of server side encryption should be used, if required
//func (psc *PayloadStorageConfiguration) WithServerSideEncryption(serverSideEncryptionStrategy types.ServerSideEncryption) *PayloadStorageConfiguration {
//	psc.ServerSideEncryption = serverSideEncryptionStrategy
//	return psc
//}
//
//// WithObjectCannedACL configures the ACL to apply to the Amazon S3 putObject request
//func (psc *PayloadStorageConfiguration) WithObjectCannedACL(ObjectCannedACL types.ObjectCannedACL) *PayloadStorageConfiguration {
//	psc.ObjectCannedACL = ObjectCannedACL
//	return psc
//}

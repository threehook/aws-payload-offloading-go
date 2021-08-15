package payload

import (
	"encoding/json"
	"errors"
	"log"
)

type PayloadS3Pointer struct {
	// private static final Logger LOG = LoggerFactory.getLogger(PayloadS3Pointer.class);
	S3BucketName string `json:"s3BucketName"`
	S3Key        string `json:"s3Key"`
}

//func NewPayloadS3Pointer(s3BucketName string, s3Key string) *PayloadS3Pointer {
//	return &PayloadS3Pointer{
//		S3BucketName: s3BucketName,
//		S3Key:        s3Key,
//	}
//}

func (psp *PayloadS3Pointer) ToJson() (string, error) {
	bytes, err := json.Marshal(psp)
	if err != nil {
		log.Println(err)
		return "", err
	}
	return string(bytes), nil
}

func FromJson(s3PointerJson string) (*PayloadS3Pointer, error) {
	var p PayloadS3Pointer
	bytes := []byte(s3PointerJson)
	err := json.Unmarshal(bytes, &p)
	if err != nil {
		log.Println(err)
		return nil, errors.New("Failed to read the S3Client object pointer from given string")
	}
	return &p, nil
}

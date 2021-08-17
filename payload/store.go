package payload

import (
	"github.com/google/uuid"
	"github.com/threehook/aws-payload-offloading-go/s3"
	"log"
)

type PayloadStore interface {
	// StoreOriginalPayload stores payload in a store that has higher payload size limit than that is supported by original payload store
	StoreOriginalPayload(payload string) (string, error)

	// StoreOriginalPayloadWithS3Key stores payload in a store that has higher payload size limit than that is supported by original payload store
	// with S3Client key
	StoreOriginalPayloadForS3Key(payload, s3Key string) (string, error)

	// GetOriginalPayload retrieves the original payload using the given payloadPointer. The pointer must have been obtained using
	// StoreOriginalPayload
	GetOriginalPayload(payloadPointer string) (string, error)

	// DeleteOriginalPayload deletes the original payload using the given payloadPointer. The pointer must have been
	// obtained using StoreOriginalPayload
	DeleteOriginalPayload(payloadPointer string) error
}

type S3BackedPayloadStore struct {
	S3BucketName string
	S3Dao        s3.S3DaoI
}

func (bps *S3BackedPayloadStore) StoreOriginalPayload(payload string) (string, error) {
	s3Key := uuid.New().String()
	return bps.StoreOriginalPayloadForS3Key(payload, s3Key)
}

func (bps *S3BackedPayloadStore) StoreOriginalPayloadForS3Key(payload, s3Key string) (string, error) {
	if err := bps.S3Dao.StoreTextInS3(bps.S3BucketName, s3Key, payload); err != nil {
		log.Println(err)
		return "", err
	}

	log.Printf("S3Client object created, Bucket name: %s, Object key:  %s.", bps.S3BucketName, s3Key) // info

	// Convert S3Client pointer (bucket name, key, etc) to JSON string
	s3Pointer := PayloadS3Pointer{bps.S3BucketName, s3Key}
	json, _ := s3Pointer.ToJson()

	return json, nil
}

func (bps *S3BackedPayloadStore) GetOriginalPayload(payloadPointer string) (string, error) {
	s3Pointer, err := FromJson(payloadPointer)
	if err != nil {
		log.Println(err)
		return "", err
	}
	s3BucketName := s3Pointer.S3BucketName
	s3Key := s3Pointer.S3Key
	originalPayload, err := bps.S3Dao.GetTextFromS3(s3BucketName, s3Key)
	if err != nil {
		log.Println(err)
		return "", err
	}

	log.Printf("S3Client object read, Bucket name: %s, Object key:  %s.", s3BucketName, s3Key) // info

	return originalPayload, nil
}

func (bps *S3BackedPayloadStore) DeleteOriginalPayload(payloadPointer string) error {
	s3Pointer, err := FromJson(payloadPointer)
	if err != nil {
		log.Println(err)
		return err
	}
	s3BucketName := s3Pointer.S3BucketName
	s3Key := s3Pointer.S3Key
	if err := bps.S3Dao.DeletePayloadFromS3(s3BucketName, s3Key); err != nil {
		log.Println(err)
		return err
	}
	return nil
}

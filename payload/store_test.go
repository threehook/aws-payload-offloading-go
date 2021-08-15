package payload

import (
	"errors"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/threehook/aws-payload-offloading-go/mocks"
	"io/ioutil"
	"log"
	"os"
	"testing"
)

const (
	s3BucketName = "test-bucket-name"
	anyS3Key     = "AnyS3key"
	anyPayload   = "AnyPayload"
)

// Suppress logging in unit tests
func TestMain(m *testing.M) {
	log.SetOutput(ioutil.Discard)
	os.Exit(m.Run())
}

func TestStoreOriginalPayloadOnSuccess(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	var capturedArgsMap = make(map[string]interface{})
	mockS3Dao.EXPECT().StoreTextInS3(s3BucketName, gomock.Any(), anyPayload).Do(
		func(s3BucketName, s3Key, payloadContentStr string) {
			capturedArgsMap["s3Key"] = s3Key
		},
	).Times(1)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	actualPayloadPointer, _ := payloadStore.StoreOriginalPayload(anyPayload)

	expectedPayloadPointer := &PayloadS3Pointer{s3BucketName, capturedArgsMap["s3Key"].(string)}

	assert := assert.New(t)
	ptrJson, _ := expectedPayloadPointer.ToJson()
	assert.Equal(ptrJson, actualPayloadPointer)
}

func TestStoreOriginalPayloadWithS3KeyOnSuccess(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	var capturedArgsMap = make(map[string]interface{})
	mockS3Dao.EXPECT().StoreTextInS3(s3BucketName, anyS3Key, anyPayload).Do(
		func(s3BucketName, s3Key, payloadContentStr string) {
			capturedArgsMap["s3Key"] = s3Key
		},
	).Times(1)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	actualPayloadPointer, _ := payloadStore.StoreOriginalPayloadForS3Key(anyPayload, anyS3Key)

	expectedPayloadPointer := &PayloadS3Pointer{s3BucketName, anyS3Key}

	assert := assert.New(t)
	ptrJson, _ := expectedPayloadPointer.ToJson()
	assert.Equal(ptrJson, actualPayloadPointer)
}

func TestStoreOriginalPayloadDoesAlwaysCreateNewObjects(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	var capturedArgsMap = make(map[string]interface{})
	firstCall := mockS3Dao.EXPECT().StoreTextInS3(s3BucketName, gomock.Any(), anyPayload)
	secondCall := mockS3Dao.EXPECT().StoreTextInS3(s3BucketName, gomock.Any(), anyPayload)

	gomock.InOrder(
		firstCall.Do(
			func(s3BucketName, s3Key, payloadContentStr string) {
				capturedArgsMap["s3Key_1"] = s3Key
			},
		),
		secondCall.Do(
			func(s3BucketName, s3Key, payloadContentStr string) {
				capturedArgsMap["s3Key_2"] = s3Key
			},
		),
	)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	//Store any payload
	anyActualPayloadPointer, _ := payloadStore.StoreOriginalPayload(anyPayload)
	//Store any other payload and validate that the pointers are different
	anyOtherActualPayloadPointer, _ := payloadStore.StoreOriginalPayload(anyPayload)

	anyExpectedPayloadPointer := &PayloadS3Pointer{s3BucketName, capturedArgsMap["s3Key_1"].(string)}
	anyOtherExpectedPayloadPointer := &PayloadS3Pointer{s3BucketName, capturedArgsMap["s3Key_2"].(string)}

	assert := assert.New(t)
	ptrJson, _ := anyExpectedPayloadPointer.ToJson()
	assert.Equal(ptrJson, anyActualPayloadPointer)
	otherPtrJson, _ := anyOtherExpectedPayloadPointer.ToJson()
	assert.Equal(otherPtrJson, anyOtherActualPayloadPointer)
	assert.NotEqual(anyExpectedPayloadPointer, anyOtherExpectedPayloadPointer)
}

func TestStoreOriginalPayloadOnS3Failure(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	mockS3Dao.EXPECT().StoreTextInS3(s3BucketName, gomock.Any(), anyPayload).Return(errors.New("Failed to store the message content in an S3Client object.")).Times(1)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	_, err := payloadStore.StoreOriginalPayload(anyPayload)

	expectedError := errors.New("Failed to store the message content in an S3Client object.")

	assert := assert.New(t)
	assert.Equal(expectedError, err)
}

func TestGetOriginalPayloadOnSuccess(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	var capturedArgsMap = make(map[string]interface{})
	mockS3Dao.EXPECT().GetTextFromS3(gomock.Any(), gomock.Any()).Return(anyPayload, nil).Do(
		func(s3BucketName, s3Key string) {
			capturedArgsMap["s3BucketName"] = s3BucketName
			capturedArgsMap["s3Key"] = s3Key
		},
	).Times(1)

	anyPointer := PayloadS3Pointer{S3BucketName: s3BucketName, S3Key: anyS3Key}
	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	ptrJson, _ := anyPointer.ToJson()
	actualPayload, _ := payloadStore.GetOriginalPayload(ptrJson)

	assert := assert.New(t)
	assert.Equal(anyS3Key, capturedArgsMap["s3Key"])
	assert.Equal(s3BucketName, capturedArgsMap["s3BucketName"])
	assert.Equal(anyPayload, actualPayload)
}

func TestGetOriginalPayloadIncorrectPointer(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	mockS3Dao.EXPECT().GetTextFromS3(gomock.Any(), gomock.Any()).Times(0)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	payloadStore.GetOriginalPayload("IncorrectPointer")
}

func TestGetOriginalPayloadOnS3Failure(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	mockS3Dao.EXPECT().GetTextFromS3(gomock.Any(), gomock.Any()).Return("", errors.New("S3Client Exception")).Times(1)

	anyPointer := PayloadS3Pointer{S3BucketName: s3BucketName, S3Key: anyS3Key}
	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	ptrJson, _ := anyPointer.ToJson()
	_, err := payloadStore.GetOriginalPayload(ptrJson)

	expectedError := errors.New("S3Client Exception")

	assert := assert.New(t)
	assert.Equal(expectedError, err)
}

func TestDeleteOriginalPayloadOnSuccess(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	var capturedArgsMap = make(map[string]interface{})
	mockS3Dao.EXPECT().DeletePayloadFromS3(gomock.Any(), gomock.Any()).Do(
		func(s3BucketName, s3Key string) {
			capturedArgsMap["s3BucketName"] = s3BucketName
			capturedArgsMap["s3Key"] = s3Key
		},
	).Times(1)

	anyPointer := PayloadS3Pointer{S3BucketName: s3BucketName, S3Key: anyS3Key}
	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	ptrJson, _ := anyPointer.ToJson()
	payloadStore.DeleteOriginalPayload(ptrJson)

	assert := assert.New(t)
	assert.Equal(anyS3Key, capturedArgsMap["s3Key"])
	assert.Equal(s3BucketName, capturedArgsMap["s3BucketName"])
}

func TestDeleteOriginalPayloadIncorrectPointer(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	mockS3Dao := mocks.NewMockS3DaoI(mockCtrl)

	mockS3Dao.EXPECT().DeletePayloadFromS3(gomock.Any(), gomock.Any()).Times(0)

	payloadStore := S3BackedPayloadStore{S3BucketName: s3BucketName, S3Dao: mockS3Dao}
	payloadStore.DeleteOriginalPayload("IncorrectPointer")
}

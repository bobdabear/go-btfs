package object

import (
	"context"
	"github.com/bittorrent/go-btfs/s3/datatypes"
	"github.com/bittorrent/go-btfs/s3/utils/hash"
	"io"
	"time"
)

type Service interface {
	//object
	SetHasBucket(hasBucket func(ctx context.Context, bucket string) bool)
	CopyObject(ctx context.Context, bucket, object string, info ObjectInfo, size int64, meta map[string]string) (ObjectInfo, error)
	StoreObject(ctx context.Context, bucket, object string, reader *hash.Reader, size int64, meta map[string]string) (ObjectInfo, error)
	PutObjectInfo(ctx context.Context, objInfo ObjectInfo) error
	GetObject(ctx context.Context, bucket, object string) (ObjectInfo, io.ReadCloser, error)
	GetObjectInfo(ctx context.Context, bucket, object string) (meta ObjectInfo, err error)
	DeleteObject(ctx context.Context, bucket, object string) error
	CleanObjectsInBucket(ctx context.Context, bucket string) error

	ListObjects(ctx context.Context, bucket string, prefix string, marker string, delimiter string, maxKeys int) (loi ListObjectsInfo, err error)
	EmptyBucket(ctx context.Context, bucket string) (bool, error)
	ListObjectsV2(ctx context.Context, bucket string, prefix string, continuationToken string, delimiter string, maxKeys int, owner bool, startAfter string) (ListObjectsV2Info, error)
	NewMultipartUpload(ctx context.Context, bucket string, object string, meta map[string]string) (MultipartInfo, error)
	GetMultipartInfo(ctx context.Context, bucket string, object string, uploadID string) (MultipartInfo, error)
	PutObjectPart(ctx context.Context, bucket string, object string, uploadID string, partID int, reader *hash.Reader, size int64, meta map[string]string) (pi objectPartInfo, err error)
	CompleteMultiPartUpload(ctx context.Context, bucket string, object string, uploadID string, parts []datatypes.CompletePart) (oi ObjectInfo, err error)
	AbortMultipartUpload(ctx context.Context, bucket string, object string, uploadID string) error

	ListMultipartUploads(ctx context.Context, bucket, prefix, keyMarker, uploadIDMarker, delimiter string, maxUploads int) (result ListMultipartsInfo, err error)
}

// ObjectInfo - represents object metadata.
//
//	{
//		Bucket = {string} "test"
//		Name = {string} "default.exe"
//		ModTime = {time.Time} 2022-03-18 10:54:43.308685163 +0800
//		Size = {int64} 11604147
//		IsDir = {bool} false
//		ETag = {string} "a6b0b7ddb4630832ed47821af59aa125"
//		Cid = {string} "QmRP168AQEN9vz8vnjWdEWiiJbNt4BZ5cB81qSRL5FQfGt"
//		VersionID = {string} ""
//		IsLatest = {bool} false
//		DeleteMarker = {bool} false
//		ContentType = {string} "application/x-msdownload"
//		ContentEncoding = {string} ""
//		Expires = {time.Time} 0001-01-01 00:00:00 +0000
//		Parts = {[]ObjectPartInfo} nil
//		AccTime = {time.Time} 0001-01-01 00:00:00 +0000
//		SuccessorModTime = {time.Time} 0001-01-01 00:00:00 +0000
//	}
type ObjectInfo struct {
	// Name of the bucket.
	Bucket string

	// Name of the object.
	Name string

	// Date and time when the object was last modified.
	ModTime time.Time

	// Total object size.
	Size int64

	// IsDir indicates if the object is prefix.
	IsDir bool

	// Hex encoded unique entity tag of the object.
	ETag string

	// ipfs key
	Cid string
	Acl string
	// Version ID of this object.
	VersionID string

	// IsLatest indicates if this is the latest current version
	// latest can be true for delete marker or a version.
	IsLatest bool

	// DeleteMarker indicates if the versionId corresponds
	// to a delete marker on an object.
	DeleteMarker bool

	// A standard MIME type describing the format of the object.
	ContentType string

	// Specifies what content encodings have been applied to the object and thus
	// what decoding mechanisms must be applied to obtain the object referenced
	// by the Content-Type header field.
	ContentEncoding string

	// Date and time at which the object is no longer able to be cached
	Expires time.Time

	// Date and time when the object was last accessed.
	AccTime time.Time

	//  The mod time of the successor object version if any
	SuccessorModTime time.Time
}

// objectPartInfo Info of each part kept in the multipart metadata
// file after CompleteMultipartUpload() is called.
type objectPartInfo struct {
	ETag    string    `json:"etag,omitempty"`
	Cid     string    `json:"cid,omitempty"`
	Number  int       `json:"number"`
	Size    int64     `json:"size"`
	ModTime time.Time `json:"mod_time"`
}

type MultipartInfo struct {
	Bucket    string
	Object    string
	UploadID  string
	Initiated time.Time
	MetaData  map[string]string
	// List of individual parts, maximum size of upto 10,000
	Parts []objectPartInfo
}

// putObjReader is a type that wraps sio.EncryptReader and
// underlying hash.Reader in a struct
type putObjReader struct {
	*hash.Reader              // actual data stream
	rawReader    *hash.Reader // original data stream
	sealMD5Fn    sealMD5CurrFn
}

// sealMD5CurrFn seals md5sum with object encryption key and returns sealed
// md5sum
type sealMD5CurrFn func([]byte) []byte

// versionPurgeStatusType represents status of a versioned delete or permanent delete w.r.t bucket replication
type versionPurgeStatusType string

const (
	// pending - versioned delete replication is pending.
	pending versionPurgeStatusType = "PENDING"

	// complete - versioned delete replication is now complete, erase version on disk.
	complete versionPurgeStatusType = "COMPLETE"

	// failed - versioned delete replication failed.
	failed versionPurgeStatusType = "FAILED"
)

// Empty returns true if purge status was not set.
func (v versionPurgeStatusType) Empty() bool {
	return string(v) == ""
}

// Pending  returns true if the version is pending purge.
func (v versionPurgeStatusType) Pending() bool {
	return v == pending || v == failed
}

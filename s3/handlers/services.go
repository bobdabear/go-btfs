package handlers

import (
	"context"
	"github.com/bittorrent/go-btfs/s3/action"
	"github.com/bittorrent/go-btfs/s3/apierrors"
	"net/http"
)

type CorsService interface {
	GetAllowOrigins() []string
	GetAllowMethods() []string
	GetAllowHeaders() []string
}

type AccessKeyService interface {
	Generate() (record *AccessKeyRecord, err error)
	Enable(key string) (err error)
	Disable(key string) (err error)
	Reset(key string) (err error)
	Delete(key string) (err error)
	Get(key string) (record *AccessKeyRecord, err error)
	List() (list []*AccessKeyRecord, err error)
}

type AuthService interface {
	VerifySignature(ctx context.Context, r *http.Request) (accessKeyRecord *AccessKeyRecord, err apierrors.ErrorCode)
	CheckACL(accessKeyRecord *AccessKeyRecord, bucketMeta *BucketMeta, action action.Action) (err error)
}

type BucketService interface {
}

type ObjectService interface {
}

type MultipartService interface {
}
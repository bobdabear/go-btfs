package handlers

import "errors"

var (
	ErrBucketNotFound        = errors.New("bucket is not found")
	ErrSginVersionNotSupport = errors.New("sign version is not support")
)
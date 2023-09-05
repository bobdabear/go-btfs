package handlers

import (
	"encoding/xml"
	"errors"
	"github.com/bittorrent/go-btfs/s3/action"
	"github.com/bittorrent/go-btfs/s3/cctx"
	"github.com/bittorrent/go-btfs/s3/consts"
	"github.com/bittorrent/go-btfs/s3/requests"
	"github.com/bittorrent/go-btfs/s3/responses"
	"github.com/bittorrent/go-btfs/s3/s3utils"
	"github.com/bittorrent/go-btfs/s3/services/object"
	"github.com/bittorrent/go-btfs/s3/utils"
	"github.com/bittorrent/go-btfs/s3/utils/hash"
	"github.com/gorilla/mux"
	"net/http"
	"path"
	"time"
)

const lockWaitTimeout = 5 * time.Minute

func (h *Handlers) PutObjectHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ack := cctx.GetAccessKey(r)
	var err error
	defer func() {
		cctx.SetHandleInf(r, h.name(), err)
	}()

	// X-Amz-Copy-Source shouldn't be set for this call.
	if _, ok := r.Header[consts.AmzCopySource]; ok {
		err = errors.New("shouldn't be copy")
		responses.WriteErrorResponse(w, r, responses.ErrInvalidCopySource)
		return
	}

	_, rerr := requests.ParseObjectACL(r)
	if rerr != nil {
		err = rerr
		responses.WriteErrorResponse(w, r, rerr)
		return
	}

	bucname, rerr := requests.ParseBucket(r)
	if rerr != nil {
		err = rerr
		responses.WriteErrorResponse(w, r, rerr)
		return
	}

	objname, rerr := requests.ParseObject(r)
	if rerr != nil {
		err = rerr
		responses.WriteErrorResponse(w, r, rerr)
	}

	err = s3utils.CheckPutObjectArgs(ctx, bucname, objname)
	if err != nil {
		rerr = h.respErr(err)
		responses.WriteErrorResponse(w, r, rerr)
		return
	}

	meta, err := extractMetadata(ctx, r)
	if err != nil {
		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequest)
		return
	}

	if r.ContentLength == 0 {
		responses.WriteErrorResponse(w, r, responses.ErrEntityTooSmall)
		return
	}

	hrdr, ok := r.Body.(*hash.Reader)
	if !ok {
		responses.WriteErrorResponse(w, r, responses.ErrInternalError)
		return
	}

	obj, err := h.objsvc.PutObject(ctx, ack, bucname, objname, hrdr, r.ContentLength, meta)
	if err != nil {
		rerr = h.respErr(err)
		responses.WriteErrorResponse(w, r, rerr)
		return
	}

	responses.WritePutObjectResponse(w, r, obj)

	return
}

//// HeadObjectHandler - HEAD Object
//func (h *Handlers) HeadObjectHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, objname, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//
//	if err := s3utils.CheckGetObjArgs(ctx, bucname, objname); err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.HeadObjectAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	// rlock object
//	runlockObj, err := h.rlock(ctx, bucname+"/"+objname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlockObj()
//
//	//objsvc
//	obj, err := h.objsvc.GetObjectInfo(ctx, bucname, objname)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	w.Header().Set(consts.AmzServerSideEncryption, consts.AmzEncryptionAES)
//
//	// Set standard object headers.
//	responses.SetObjectHeaders(w, r, obj)
//	// Set any additional requested response headers.
//	responses.SetHeadGetRespHeaders(w, r.Form)
//
//	// Successful response.
//	w.WriteHeader(http.StatusOK)
//}
//
//// CopyObjectHandler - Copy Object
//func (h *Handlers) CopyObjectHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	dstBucket, dstObject, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//	if err := s3utils.CheckPutObjectArgs(ctx, dstBucket, dstObject); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	err = h.bucsvc.CheckACL(ack, dstBucket, action.CopyObjectAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// Copy source path.
//	cpSrcPath, err := url.QueryUnescape(r.Header.Get(consts.AmzCopySource))
//	if err != nil {
//		// Save unescaped string as is.
//		cpSrcPath = r.Header.Get(consts.AmzCopySource)
//	}
//	srcBucket, srcObject := pathToBucketAndObject(cpSrcPath)
//	// If source object is empty or bucket is empty, reply back invalid copy source.
//	if srcObject == "" || srcBucket == "" {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidCopySource)
//		return
//	}
//	if err = s3utils.CheckGetObjArgs(ctx, srcBucket, srcObject); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	if srcBucket == dstBucket && srcObject == dstObject {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidCopyDest)
//		return
//	}
//	err = h.bucsvc.CheckACL(ack, srcBucket, action.CopyObjectAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	fmt.Printf("CopyObjectHandler %s %s => %s %s \n", srcBucket, srcObject, dstBucket, dstObject)
//
//	// rlock bucket 1
//	runlock1, err := h.rlock(ctx, srcBucket, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock1()
//
//	// rlock object 1
//	runlockObj1, err := h.rlock(ctx, srcBucket+"/"+srcObject, w, r)
//	if err != nil {
//		return
//	}
//	defer runlockObj1()
//
//	// rlock bucket 2
//	runlock2, err := h.rlock(ctx, dstBucket, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock2()
//
//	// lock object 2
//	unlockObj2, err := h.lock(ctx, dstBucket+"/"+dstObject, w, r)
//	if err != nil {
//		return
//	}
//	defer unlockObj2()
//
//	//objsvc
//	srcObjInfo, err := h.objsvc.GetObjectInfo(ctx, srcBucket, srcObject)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	metadata := make(map[string]string)
//	metadata[strings.ToLower(consts.ContentType)] = srcObjInfo.ContentType
//	metadata[strings.ToLower(consts.ContentEncoding)] = srcObjInfo.ContentEncoding
//	if isReplace(r) {
//		inputMeta, err := extractMetadata(ctx, r)
//		if err != nil {
//			responses.WriteErrorResponse(w, r, err)
//			return
//		}
//		for key, val := range inputMeta {
//			metadata[key] = val
//		}
//	}
//
//	//objsvc
//	obj, err := h.objsvc.CopyObject(ctx, dstBucket, dstObject, srcObjInfo, srcObjInfo.Size, metadata)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	resp := responses.CopyObjectResult{
//		ETag:         "\"" + obj.ETag + "\"",
//		LastModified: obj.ModTime.UTC().Format(consts.Iso8601TimeFormat),
//	}
//
//	setPutObjHeaders(w, obj, false)
//
//	responses.WriteSuccessResponseXML(w, r, resp)
//}
//
//// DeleteObjectHandler - delete an object
//// https://docs.aws.amazon.com/AmazonS3/latest/API/API_DeleteObject.html
//func (h *Handlers) DeleteObjectHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, objname, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//	if err := s3utils.CheckDelObjArgs(ctx, bucname, objname); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.DeleteObjectAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	// lock object
//	unlock, err := h.lock(ctx, bucname+"/"+objname, w, r)
//	if err != nil {
//		return
//	}
//	defer unlock()
//
//	//objsvc
//	obj, err := h.objsvc.GetObjectInfo(ctx, bucname, objname)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	//objsvc
//	err = h.objsvc.DeleteObject(ctx, bucname, objname)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	setPutObjHeaders(w, obj, true)
//	responses.WriteSuccessNoContent(w)
//}
//

func trimLeadingSlash(ep string) string {
	if len(ep) > 0 && ep[0] == '/' {
		// Path ends with '/' preserve it
		if ep[len(ep)-1] == '/' && len(ep) > 1 {
			ep = path.Clean(ep)
			ep += "/"
		} else {
			ep = path.Clean(ep)
		}
		ep = ep[1:]
	}
	return ep
}

// DeletedObject objects deleted
type DeletedObject struct {
	DeleteMarker          bool   `xml:"DeleteMarker,omitempty"`
	DeleteMarkerVersionID string `xml:"DeleteMarkerVersionId,omitempty"`
	ObjectName            string `xml:"Key,omitempty"`
	VersionID             string `xml:"VersionId,omitempty"`
}

// ObjectV object version key/versionId
type ObjectV struct {
	ObjectName string `xml:"Key"`
	VersionID  string `xml:"VersionId"`
}

// ObjectToDelete carries key name for the object to delete.
type ObjectToDelete struct {
	ObjectV
}

// DeleteObjectsRequest - xml carrying the object key names which needs to be deleted.
type DeleteObjectsRequest struct {
	// Element to enable quiet mode for the request
	Quiet bool
	// List of objects to be deleted
	Objects []ObjectToDelete `xml:"Object"`
}

type DeleteMultipleObjectsRequest struct {
	BucName          string
	ObjName          string
	DeleteObjectsReq DeleteObjectsRequest
}

func parseReqDeleteMultipleObjects(r *http.Request) (req *DeleteMultipleObjectsRequest, err error) {
	req = &DeleteMultipleObjectsRequest{}

	req.BucName = mux.Vars(r)["bucket"]

	// Content-Md5/Content-Length is requied should be set
	if _, ok := r.Header[consts.ContentMD5]; !ok {
		err = responses.ErrMissingContentMD5
		return
	}
	if r.ContentLength <= 0 {
		err = responses.ErrMissingContentLength
		return
	}

	// The max. XML contains 100000 object names (each at most 1024 bytes long) + XML overhead
	const maxBodySize = 2 * 100000 * 1024

	// Unmarshal list of keys to be deleted.
	deleteObjectsReq := DeleteObjectsRequest{}
	if err := utils.XmlDecoder(r.Body, deleteObjectsReq, maxBodySize); err != nil {
		err = responses.ErrMalformedXML
		return
	}

	// Convert object name delete objects if it has `/` in the beginning.
	for i := range deleteObjectsReq.Objects {
		deleteObjectsReq.Objects[i].ObjectName = trimLeadingSlash(deleteObjectsReq.Objects[i].ObjectName)
	}

	// Return Malformed XML as S3 spec if the number of objects is empty
	if len(deleteObjectsReq.Objects) == 0 || len(deleteObjectsReq.Objects) > consts.MaxDeleteList {
		err = responses.ErrMalformedXML
		return
	}

	req.DeleteObjectsReq = deleteObjectsReq
	return
}

// DeleteError structure.
type DeleteError struct {
	Code      string
	Message   string
	Key       string
	VersionID string `xml:"VersionId"`
}

// DeleteObjectsResponse container for multiple object deletes.
type DeleteObjectsResponse struct {
	XMLName xml.Name `xml:"http://s3.amazonaws.com/doc/2006-03-01/ DeleteResult" json:"-"`

	// Collection of all deleted objects
	DeletedObjects []DeletedObject `xml:"Deleted,omitempty"`

	// Collection of errors deleting certain objects.
	Errors []DeleteError `xml:"Error,omitempty"`
}

type deleteResult struct {
	delInfo DeletedObject
	errInfo DeleteError
}

func packRespDeleteMultipleObjects(deleteList []ObjectToDelete, dObjects []DeletedObject, errs []error, quiet bool) (resp DeleteObjectsResponse) {
	deleteResults := make([]deleteResult, len(deleteList))
	for i := range errs {
		if errs[i] == nil {
			deleteResults[i].delInfo = dObjects[i]
			continue
		}
		//apiErrCode := apierrors.ToApiError(ctx, errs[i])
		//apiErr := apierrors.GetAPIError(apiErrCode)

		code := 1
		message := "err"

		deleteResults[i].errInfo = DeleteError{
			Code:      code,
			Message:   message,
			Key:       deleteList[i].ObjectName,
			VersionID: deleteList[i].VersionID,
		}
	}

	// Generate response
	deleteErrors := make([]error, 0, len(deleteList))
	deletedObjects := make([]DeletedObject, 0, len(deleteList))
	for _, deleteResult := range deleteResults {
		if deleteResult.errInfo.Code != "" {
			deleteErrors = append(deleteErrors, deleteResult.errInfo)
		} else {
			deletedObjects = append(deletedObjects, deleteResult.delInfo)
		}
	}

	resp = DeleteObjectsResponse{}
	if !quiet {
		resp.DeletedObjects = deletedObjects
	}
	resp.Errors = errs
	return resp
}

// DeleteMultipleObjectsHandler - Delete multiple objects
func (h *Handlers) DeleteMultipleObjectsHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	ack := cctx.GetAccessKey(r)
	var err error
	defer func() {
		cctx.SetHandleInf(r, h.name(), err)
	}()

	req, err := parseReqDeleteMultipleObjects(r)
	if err != nil {
		responses.WriteErrorResponse(w, r, err)
		return
	}
	bucname, deleteObjectsReq := req.BucName, req.DeleteObjectsReq

	err = h.bucsvc.CheckACL(ack, bucname, action.DeleteObjectsAction)
	if errors.Is(err, object.ErrBucketNotFound) {
		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
		return
	}
	if err != nil {
		responses.WriteErrorResponse(w, r, err)
		return
	}

	// rlock bucket
	runlock, err := h.rlock(ctx, bucname, w, r)
	if err != nil {
		return
	}
	defer runlock()

	// lock object
	unlock, err := h.lock(ctx, bucname+"/"+objname, w, r)
	if err != nil {
		return
	}
	defer unlock()

	//delete objects
	ctx = r.Context()
	deleteList := deleteObjectsReq.Objects //toNames(objectsToDelete)
	dObjects := make([]DeletedObject, len(deleteList))
	errs := make([]error, len(deleteList))
	for i, obj := range deleteList {
		if errs[i] = s3utils.CheckDelObjArgs(ctx, bucname, obj.ObjectName); errs[i] != nil {
			continue
		}
		errs[i] = h.objsvc.DeleteObject(ctx, ack, bucname, obj.ObjectName)
		if errs[i] == nil {
			dObjects[i] = DeletedObject{
				ObjectName: obj.ObjectName,
			}
			errs[i] = nil
		}
	}

	resp := packRespDeleteMultipleObjects(deleteList, dObjects, errs, deleteObjectsReq.Quiet)
	responses.WriteSuccessResponseXML(w, r, resp)
}

//
//// GetObjectHandler - GET Object
//// https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
//func (h *Handlers) GetObjectHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, objname, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//	if err = s3utils.CheckGetObjArgs(ctx, bucname, objname); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.GetObjectAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	// rlock object
//	runlockObj, err := h.rlock(ctx, bucname+"/"+objname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlockObj()
//
//	//objsvc
//	obj, reader, err := h.objsvc.GetObject(ctx, bucname, objname)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	//w.Header().Set(consts.AmzServerSideEncryption, consts.AmzEncryptionAES)
//
//	responses.SetObjectHeaders(w, r, obj)
//	w.Header().Set(consts.ContentLength, strconv.FormatInt(obj.Size, 10))
//	responses.SetHeadGetRespHeaders(w, r.Form)
//	_, err = io.Copy(w, reader)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInternalError)
//		return
//	}
//}
//
//// GetObjectACLHandler - GET Object ACL
//func (h *Handlers) GetObjectACLHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, _, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.GetBucketAclAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	acl, err := h.bucsvc.GetBucketACL(ctx, bucname)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	responses.WriteGetBucketAclResponse(w, r, ack, acl)
//}
//
//func (h *Handlers) ListObjectsHandler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, _, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//
//	// Extract all the litsObjectsV1 query params to their native values.
//	prefix, marker, delimiter, maxKeys, encodingType, s3Error := getListObjectsV1Args(r.Form)
//	if s3Error != nil {
//		responses.WriteErrorResponse(w, r, s3Error)
//		return
//	}
//
//	if err := s3utils.CheckListObjsArgs(ctx, bucname, prefix, marker); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.ListObjectsAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	//objsvc
//	objs, err := h.objsvc.ListObjects(ctx, bucname, prefix, marker, delimiter, maxKeys)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//	resp := responses.GenerateListObjectsV1Response(bucname, prefix, marker, delimiter, encodingType, maxKeys, objs)
//	// Write success response.
//	responses.WriteSuccessResponseXML(w, r, resp)
//}
//
//func (h *Handlers) ListObjectsV2Handler(w http.ResponseWriter, r *http.Request) {
//	ctx := r.Context()
//	ack := cctx.GetAccessKey(r)
//	var err error
//	defer func() {
//		cctx.SetHandleInf(r, h.name(), err)
//	}()
//
//	bucname, _, err := requests.ParseBucketAndObject(r)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, responses.ErrInvalidRequestParameter)
//		return
//	}
//
//	err = h.bucsvc.CheckACL(ack, bucname, action.ListObjectsAction)
//	if errors.Is(err, object.ErrBucketNotFound) {
//		responses.WriteErrorResponse(w, r, responses.ErrNoSuchBucket)
//		return
//	}
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	urlValues := r.Form
//	// Extract all the listObjectsV2 query params to their native values.
//	prefix, token, startAfter, delimiter, fetchOwner, maxKeys, encodingType, errCode := getListObjectsV2Args(urlValues)
//	if errCode != nil {
//		responses.WriteErrorResponse(w, r, errCode)
//		return
//	}
//
//	marker := token
//	if marker == "" {
//		marker = startAfter
//	}
//	if err := s3utils.CheckListObjsArgs(ctx, bucname, prefix, marker); err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	// Validate the query params before beginning to serve the request.
//	// fetch-owner is not validated since it is a boolean
//	s3Error := validateListObjectsArgs(token, delimiter, encodingType, maxKeys)
//	if s3Error != nil {
//		responses.WriteErrorResponse(w, r, s3Error)
//		return
//	}
//
//	// rlock bucket
//	runlock, err := h.rlock(ctx, bucname, w, r)
//	if err != nil {
//		return
//	}
//	defer runlock()
//
//	// Initiate a list objects operation based on the input params.
//	// On success would return back ListObjectsInfo object to be
//	// marshaled into S3 compatible XML header.
//	//objsvc
//	listObjectsV2Info, err := h.objsvc.ListObjectsV2(ctx, bucname, prefix, token, delimiter,
//		maxKeys, fetchOwner, startAfter)
//	if err != nil {
//		responses.WriteErrorResponse(w, r, err)
//		return
//	}
//
//	resp := responses.GenerateListObjectsV2Response(
//		bucname, prefix, token, listObjectsV2Info.NextContinuationToken, startAfter,
//		delimiter, encodingType, listObjectsV2Info.IsTruncated,
//		maxKeys, listObjectsV2Info.Objects, listObjectsV2Info.Prefixes)
//
//	// Write success response.
//	responses.WriteSuccessResponseXML(w, r, resp)
//}
//
//// setPutObjHeaders sets all the necessary headers returned back
//// upon a success Put/Copy/CompleteMultipart/Delete requests
//// to activate delete only headers set delete as true
//func setPutObjHeaders(w http.ResponseWriter, obj object.Object, delete bool) {
//	// We must not use the http.Header().Set method here because some (broken)
//	// clients expect the ETag header key to be literally "ETag" - not "Etag" (case-sensitive).
//	// Therefore, we have to set the ETag directly as map entry.
//	if obj.ETag != "" && !delete {
//		w.Header()[consts.ETag] = []string{`"` + obj.ETag + `"`}
//	}
//
//	// Set the relevant version ID as part of the response header.
//	if obj.VersionID != "" {
//		w.Header()[consts.AmzVersionID] = []string{obj.VersionID}
//		// If version is a deleted marker, set this header as well
//		if obj.DeleteMarker && delete { // only returned during delete object
//			w.Header()[consts.AmzDeleteMarker] = []string{strconv.FormatBool(obj.DeleteMarker)}
//		}
//	}
//
//	if obj.Bucket != "" && obj.Name != "" {
//		// do something
//	}
//}
//
//func pathToBucketAndObject(path string) (bucket, object string) {
//	path = strings.TrimPrefix(path, consts.SlashSeparator)
//	idx := strings.Index(path, consts.SlashSeparator)
//	if idx < 0 {
//		return path, ""
//	}
//	return path[:idx], path[idx+len(consts.SlashSeparator):]
//}
//
//func isReplace(r *http.Request) bool {
//	return r.Header.Get("X-Amz-Metadata-Directive") == "REPLACE"
//}
//
//// Parse bucket url queries
//func getListObjectsV1Args(values url.Values) (
//	prefix, marker, delimiter string, maxkeys int, encodingType string, errCode error) {
//
//	if values.Get("max-keys") != "" {
//		var err error
//		if maxkeys, err = strconv.Atoi(values.Get("max-keys")); err != nil {
//			errCode = responses.ErrInvalidMaxKeys
//			return
//		}
//	} else {
//		maxkeys = consts.MaxObjectList
//	}
//
//	prefix = trimLeadingSlash(values.Get("prefix"))
//	marker = trimLeadingSlash(values.Get("marker"))
//	delimiter = values.Get("delimiter")
//	encodingType = values.Get("encoding-type")
//	return
//}
//
//// Parse bucket url queries for ListObjects V2.
//func getListObjectsV2Args(values url.Values) (
//	prefix, token, startAfter, delimiter string,
//	fetchOwner bool, maxkeys int, encodingType string, errCode error) {
//
//	// The continuation-token cannot be empty.
//	if val, ok := values["continuation-token"]; ok {
//		if len(val[0]) == 0 {
//			errCode = responses.ErrInvalidToken
//			return
//		}
//	}
//
//	if values.Get("max-keys") != "" {
//		var err error
//		if maxkeys, err = strconv.Atoi(values.Get("max-keys")); err != nil {
//			errCode = responses.ErrInvalidMaxKeys
//			return
//		}
//		// Over flowing count - reset to maxObjectList.
//		if maxkeys > consts.MaxObjectList {
//			maxkeys = consts.MaxObjectList
//		}
//	} else {
//		maxkeys = consts.MaxObjectList
//	}
//
//	prefix = trimLeadingSlash(values.Get("prefix"))
//	startAfter = trimLeadingSlash(values.Get("start-after"))
//	delimiter = values.Get("delimiter")
//	fetchOwner = values.Get("fetch-owner") == "true"
//	encodingType = values.Get("encoding-type")
//
//	if token = values.Get("continuation-token"); token != "" {
//		decodedToken, err := base64.StdEncoding.DecodeString(token)
//		if err != nil {
//			errCode = responses.ErrIncorrectContinuationToken
//			return
//		}
//		token = string(decodedToken)
//	}
//	return
//}
//
//func trimLeadingSlash(ep string) string {
//	if len(ep) > 0 && ep[0] == '/' {
//		// Path ends with '/' preserve it
//		if ep[len(ep)-1] == '/' && len(ep) > 1 {
//			ep = path.Clean(ep)
//			ep += "/"
//		} else {
//			ep = path.Clean(ep)
//		}
//		ep = ep[1:]
//	}
//	return ep
//}
//
//// Validate all the ListObjects query arguments, returns an APIErrorCode
//// if one of the args do not meet the required conditions.
////   - delimiter if set should be equal to '/', otherwise the request is rejected.
////   - marker if set should have a common prefix with 'prefix' param, otherwise
////     the request is rejected.
//func validateListObjectsArgs(marker, delimiter, encodingType string, maxKeys int) error {
//	// Max keys cannot be negative.
//	if maxKeys < 0 {
//		return responses.ErrInvalidMaxKeys
//	}
//
//	if encodingType != "" {
//		// AWS S3 spec only supports 'url' encoding type
//		if !strings.EqualFold(encodingType, "url") {
//			return responses.ErrInvalidEncodingMethod
//		}
//	}
//
//	return nil
//}
//

package rocksdbcloud

// #include <stdlib.h>
// #include "rocksdb/c.h"
import "C"
import (
	"unsafe"
	"errors"
)

// Env is a system call environment used by a database.
type Env struct {
	c *C.rocksdb_env_t
}


type CloudEnv struct {
	c *C.rocksdb_cloudenv_t
}

type CloudEnvOptions struct {
	c *C.rocksdb_cloudenv_options_t
	cc 	*C.rocksdb_options_t
	env *Env
}


func NewNativeCloudEnvOptions(c *C.rocksdb_cloudenv_options_t, cc *C.rocksdb_options_t) *CloudEnvOptions {
	return &CloudEnvOptions{c: c, cc : cc}
}


// NewCloudEnvOptions creates a default cloud environment Options
func NewCloudEnvOptions() *CloudEnvOptions {
	return NewNativeCloudEnvOptions(C.rocksdb_cloudenv_options_create(), C.rocksdb_options_create());
}


// Destroy deallocates the CloudEnvOptions object.
func (opts *CloudEnvOptions) Destroy() {
	C.rocksdb_cloudenv_options_destroy(opts.c)
	C.rocksdb_options_destroy(opts.cc)
	opts.c = nil
	opts.cc = nil
	opts.env = nil
}


func NewNativeCloudEnv(c *C.rocksdb_cloudenv_t) *CloudEnv{
	return &CloudEnv{c:c}
}

func NewAwsCloudEnv(src_bucket_name string,
	src_object_prefix string,
	src_bucket_region string,
	dest_bucket_name string,
	dest_object_prefix string,
	dest_bucket_region string,
	opts *CloudEnvOptions) (*CloudEnv,error) {
	var (
		cErr  *C.char
		src_object = C.CString(src_object_prefix)
		src_region = C.CString(src_bucket_region)
		src_bucket = C.CString(src_bucket_name)
		dst_object = C.CString(dest_object_prefix)
		dst_region = C.CString(dest_bucket_region)
		dst_bucket = C.CString(dest_bucket_name)
	)

	cloudenv := C.rocksdb_create_aws_env(opts.c,src_bucket,src_object,src_region,dst_bucket,dst_object,dst_region,&cErr)
	if cErr != nil {
		defer C.rocksdb_free(unsafe.Pointer(cErr))
		return nil, errors.New(C.GoString(cErr))
	}
	return NewNativeCloudEnv(cloudenv),nil

}

// Destroy deallocates the CloudEnvOptions object.
func (env *CloudEnv) Destroy() {
	C.rocksdb_cloudenv_destroy(env.c)
	env.c = nil
}


func (opts *CloudEnvOptions) SetEndPoint(endpoint string) {
	cendpoint := C.CString(endpoint)
	defer C.free(unsafe.Pointer(cendpoint))

	C.rocksdb_cloudenv_options_set_endpoint(opts.c, cendpoint)
}


func (opts *CloudEnvOptions) SetRequestTimeOut(timeout uint64) {
	ctimeout := C.uint64_t(timeout)
	C.rocksdb_cloudenv_options_set_request_timeout(opts.c,  ctimeout)
}


func (opts *CloudEnvOptions) SetCredentials(access_key_id string, secret_access_key string) {
	caccess_key_id := C.CString(access_key_id)
	csecret_access_key := C.CString(secret_access_key)

	defer C.free(unsafe.Pointer(caccess_key_id))
	defer C.free(unsafe.Pointer(csecret_access_key))

	C.rocksdb_cloudenv_options_set_credentials(opts.c, caccess_key_id,csecret_access_key)
}

func (opts *CloudEnvOptions) SetSrcBucket(bucket string, prefix string) {
	cbucket := C.CString(bucket)
	defer C.free(unsafe.Pointer(cbucket))

	cprefix := C.CString(bucket)
	defer C.free(unsafe.Pointer(cprefix))

	C.rocksdb_cloudenv_options_set_srcbucket(opts.c,cbucket,cprefix)
}

func (opts *CloudEnvOptions) SetDstBucket(bucket string, prefix string) {
	cbucket := C.CString(bucket)
	defer C.free(unsafe.Pointer(cbucket))

	cprefix := C.CString(bucket)
	defer C.free(unsafe.Pointer(cprefix))

	C.rocksdb_cloudenv_options_set_dstbucket(opts.c,cbucket,cprefix)
}

func (opts *CloudEnvOptions) SetCreateIfMissing(value bool) {
	var b = 0
	if value {
		b = 1
	}
	C.rocksdb_options_set_create_if_missing(opts.cc, C.uchar(b))
}

func (opts *CloudEnvOptions) SetEnv(env *Env) {
	opts.env = env
	C.rocksdb_options_set_env(opts.cc, env.c)
}
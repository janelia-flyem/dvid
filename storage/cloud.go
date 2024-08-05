package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"

	"gocloud.dev/blob"
)

// OpenBucket returns a blob.Bucket for the given reference.
// The reference should be of the form:
//
//	gcs://<bucketname>
//	s3://<bucketname>
//	vast://<endpoint>/<bucketname>
//
// If no service prefix is used, we assume GCS due to legacy support.
func OpenBucket(ref string) (bucket *blob.Bucket, err error) {
	ctx := context.Background()

	dvid.Infof("Opening bucket for %q ...\n", ref)
	var service string
	var pathParts []string
	serviceParts := strings.Split(ref, "//")
	if len(serviceParts) == 1 {
		service = "gs:"
		pathParts = strings.SplitN(ref, "/", 2)
	} else {
		service = serviceParts[0]
		pathParts = strings.SplitN(serviceParts[1], "/", 2)
	}

	switch service {
	case "vast:":
		// VAST S3-compatible storage
		// The ref should be of form "vast://<endpoint>/<bucket>" where
		// <endpoint> is the VAST endpoint and <bucket> is the bucket name.
		// The following endpoints must be set:
		//  AWS_REGION: ignored but must be set for this cross-cloud library
		//  AWS_SHARED_CREDENTIALS_FILE: path to a file with AWS credentials that
		//     should be in form:
		//	 [default]
		//	 aws_access_key_id = <access key>
		//	 aws_secret_access_key = <secret key>
		if len(pathParts) != 2 {
			return nil, fmt.Errorf("vast ref must be of form 'vast://<endpoint>/<bucket>'")
		}
		endpoint := pathParts[0]
		bucketname := pathParts[1]
		url := fmt.Sprintf("s3://%s?endpoint=%s&s3ForcePathStyle=true", bucketname, endpoint)
		bucket, err = blob.OpenBucket(ctx, url)
		return

	case "gs:","s3:":
		bucket, err = blob.OpenBucket(ctx, service+ "//" + pathParts[0])
		if err != nil {
			return nil, err
		}
		if len(pathParts) > 1 {
			path := pathParts[1]
			if len(path) == 0 || path[len(path)-1] != '/' {
				path += "/"
			}
			bucket = blob.PrefixedBucket(bucket, path)
		}
		return

	default:
		return nil, fmt.Errorf("unknown service (allow 's3:', 'gs:', 'vast:'): %s", service)
	}
}

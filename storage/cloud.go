package storage

import (
	"context"
	"fmt"
	"strings"

	"github.com/janelia-flyem/dvid/dvid"

	"gocloud.dev/blob"
	"gocloud.dev/blob/gcsblob"
	"gocloud.dev/gcp"
)

// OpenBucket returns a blob.Bucket for the given reference.
// The reference should be of the form:
//
//	gcs://<bucketname>
//	s3://<bucketname>
//	vast://<endpoint>/<bucketname>
func OpenBucket(ref string) (bucket *blob.Bucket, err error) {
	ctx := context.Background()

	if strings.HasPrefix(ref, "s3://") {
		// S3 handler contributed by Flatiron (pgunn)
		// This relies on the non-GCS-specific blob API and requires that the user:
		// A: Have set up AWS credentials in ways gocloud can find them (see the "aws config" command)
		// B: Have set the AWS_REGION environment variable (usually to us-east-2)
		bucket, err = blob.OpenBucket(ctx, ref)
		if err != nil {
			dvid.Errorf("Can't open bucket reference @ %q: %v\n", ref, err)
			return nil, err
		}
		pathpart := strings.TrimPrefix(ref, "s3://")
		pathpart = strings.SplitN(pathpart, "/", 2)[1] // Remove the bucket name
		bucket = blob.PrefixedBucket(bucket, pathpart)

	} else if strings.HasPrefix(ref, "vast://") {
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
		ref := strings.TrimPrefix(ref, "vast://")
		ref_parts := strings.SplitN(ref, "/", 2)
		if len(ref_parts) != 2 {
			return nil, fmt.Errorf("vast ref must be of form 'vast://<endpoint>/<bucket>'")
		}
		endpoint := ref_parts[0]
		bucketname := ref_parts[1]
		url := fmt.Sprintf("s3://%s?endpoint=%s&s3ForcePathStyle=true", bucketname, endpoint)
		bucket, err = blob.OpenBucket(ctx, url)
		if err != nil {
			dvid.Errorf("Can't open bucket reference @ %q: %v\n", ref, err)
			return nil, err
		}

	} else {
		// In this case default to Google Store authentication as DVID did before
		// See https://cloud.google.com/docs/authentication/production
		// for more info on alternatives.
		creds, err := gcp.DefaultCredentials(ctx)
		if err != nil {
			return nil, err
		}

		// Create an HTTP client.
		// This example uses the default HTTP transport and the credentials
		// created above.
		client, err := gcp.NewHTTPClient(
			gcp.DefaultTransport(),
			gcp.CredentialsTokenSource(creds))
		if err != nil {
			return nil, err
		}

		// Create a *blob.Bucket.
		bucket, err = gcsblob.OpenBucket(ctx, client, ref, nil)
		if err != nil {
			fmt.Printf("Can't open bucket reference @ %q: %v\n", ref, err)
			return nil, err
		}
	}
	return bucket, nil
}

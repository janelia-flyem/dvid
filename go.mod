module github.com/janelia-flyem/dvid

go 1.18

require (
	cloud.google.com/go/bigtable v1.13.0
	cloud.google.com/go/storage v1.28.1
	github.com/BurntSushi/toml v1.0.0
	github.com/DmitriyVTitov/size v1.5.0
	github.com/Shopify/sarama v1.32.0
	github.com/blang/semver v3.5.1+incompatible
	github.com/coocood/freecache v1.2.1
	github.com/dgraph-io/badger/v3 v3.2103.2
	github.com/dustin/go-humanize v1.0.0
	github.com/golang-jwt/jwt/v4 v4.4.1
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/golang/snappy v0.0.4
	github.com/janelia-flyem/go v0.0.0-20180718195536-d388bdc31871
	github.com/janelia-flyem/protolog v0.0.0-20191102211808-ce1a9ba02c03
	github.com/natefinch/lumberjack v2.0.0+incompatible
	github.com/ncw/swift v1.0.53
	github.com/rs/cors v1.8.2
	github.com/santhosh-tekuri/jsonschema/v5 v5.0.1
	github.com/tinylib/msgp v1.1.6
	github.com/twinj/uuid v1.0.0
	github.com/valyala/gorpc v0.0.0-20160519171614-908281bef774
	github.com/wblakecaldwell/profiler v0.0.0-20150908040756-6111ef1313a1
	github.com/zenazn/goji v1.0.1
	gocloud.dev v0.24.0
	golang.org/x/net v0.17.0
	golang.org/x/oauth2 v0.7.0
	google.golang.org/api v0.114.0
	google.golang.org/appengine v1.6.7
	google.golang.org/grpc v1.56.3
	google.golang.org/protobuf v1.30.0
)

require (
	cloud.google.com/go v0.110.0 // indirect
	cloud.google.com/go/compute v1.19.1 // indirect
	cloud.google.com/go/compute/metadata v0.2.3 // indirect
	cloud.google.com/go/iam v0.13.0 // indirect
	cloud.google.com/go/longrunning v0.4.1 // indirect
	github.com/aws/aws-sdk-go v1.40.34 // indirect
	github.com/aws/aws-sdk-go-v2 v1.9.0 // indirect
	github.com/aws/aws-sdk-go-v2/config v1.7.0 // indirect
	github.com/aws/aws-sdk-go-v2/credentials v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/feature/ec2/imds v1.5.0 // indirect
	github.com/aws/aws-sdk-go-v2/internal/ini v1.2.2 // indirect
	github.com/aws/aws-sdk-go-v2/service/internal/presigned-url v1.3.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sso v1.4.0 // indirect
	github.com/aws/aws-sdk-go-v2/service/sts v1.7.0 // indirect
	github.com/aws/smithy-go v1.8.0 // indirect
	github.com/census-instrumentation/opencensus-proto v0.4.1 // indirect
	github.com/cespare/xxhash v1.1.0 // indirect
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/cncf/udpa/go v0.0.0-20220112060539-c52dc94e7fbe // indirect
	github.com/cncf/xds/go v0.0.0-20230607035331-e9ce68804cb4 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/dgraph-io/ristretto v0.1.0 // indirect
	github.com/eapache/go-resiliency v1.2.0 // indirect
	github.com/eapache/go-xerial-snappy v0.0.0-20180814174437-776d5712da21 // indirect
	github.com/eapache/queue v1.1.0 // indirect
	github.com/envoyproxy/go-control-plane v0.11.1-0.20230524094728-9239064ad72f // indirect
	github.com/envoyproxy/protoc-gen-validate v0.10.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/glog v1.1.0 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/google/btree v1.0.1 // indirect
	github.com/google/flatbuffers v1.12.1 // indirect
	github.com/google/go-cmp v0.5.9 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/google/wire v0.5.0 // indirect
	github.com/googleapis/enterprise-certificate-proxy v0.2.3 // indirect
	github.com/googleapis/gax-go/v2 v2.7.1 // indirect
	github.com/hashicorp/go-uuid v1.0.2 // indirect
	github.com/jcmturner/aescts/v2 v2.0.0 // indirect
	github.com/jcmturner/dnsutils/v2 v2.0.0 // indirect
	github.com/jcmturner/gofork v1.0.0 // indirect
	github.com/jcmturner/gokrb5/v8 v8.4.2 // indirect
	github.com/jcmturner/rpc/v2 v2.0.3 // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/klauspost/compress v1.14.4 // indirect
	github.com/myesui/uuid v1.0.0 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pierrec/lz4 v2.6.1+incompatible // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rcrowley/go-metrics v0.0.0-20201227073835-cf1acfcdf475 // indirect
	go.opencensus.io v0.24.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sys v0.15.0 // indirect
	golang.org/x/text v0.14.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/genproto v0.0.0-20230410155749-daa745c078e1 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0 // indirect
	gopkg.in/stretchr/testify.v1 v1.2.2 // indirect
	launchpad.net/gocheck v0.0.0-20140225173054-000000000087 // indirect
	rsc.io/binaryregexp v0.2.0 // indirect
)

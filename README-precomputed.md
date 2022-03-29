DVID Precomputed support
====

DVID has a mode where it works as a frontend to data stored in the [precomputed format](https://neurodata.io/help/precomputed/) in a
Google Store or Amazon S3 bucket. In this configuration, DVID will have a single repo, and it will need to run in an environment where
it has credentials and configuration to access those buckets (through environment variables or dotfiles).

Before you start
====
# Ensure you have access to the bucket using the commandline tools (gcloud or awscli). If you are using AWS S3, you will additionally need to set the "AWS_REGION" environment variable (usually to us-east-2).
# For the key and secret key, it is good security practice to limit the IAM permissions to readonly credentials locked to the bucket you are serving. Avoid overbroad permissions
# Test access using the commandline tools before you start with DVID
# Ensure that the precomputed volume is complete and consistent. All described scales of the data from the info file must be present in the bucket; any missing scales will create problems. If the bucket has not finished being populated, wait for that before you start configuring DVID.

Getting started
====
Write a config.toml containing the needed configuration to serve from your bucket. A sample config is available below.

```
[server]
httpAddress = "localhost:8000"
rpcAddress = "localhost:8001"
webClient = "/home/katz/dvid-webclient"

[logging]
logfile = "/data/dvid/log"
max_log_size = 500
max_log_age = 30

[backend]
    [backend.default]
    store = "mainstore"
    log = "mutationlog"

[store]
    [store.mainstore]
    engine = "basholeveldb"
    path = "/data/dvid/db/leveldb"

    [store.mutationlog]
    engine = "filelog"
    path = "/data/dvid/db/mlog-leveldb"

    [store.mystorage]
    engine = "ngprecomputed"
    ref = "s3://mybucket/sample1/image_aligned/"
    instance = "sample1"
```

Adjust paths to match your server configuration. Note the following two parameters:

* ref should contain a bucketname and path to your data (the "info" file should be in the directory). If you include a s3:// prefix then the s3 protocol is used, otherwise a google store bucket will be used.
* instance is the name of the automatic data instance in your repo that your data will be exposed under.


Starting dvid
====
With this configuration, you can start DVID (ideally on a fresh directory/database, because DVID in this mode is meant to be used in a single-repo configuration. You will see in the logs that DVID will investigate the bucket and parse its config file. Pay attention to the logs; if you see errors, do not proceed; fix the problems. If everything goes well, it will find all the scales of your data, but it won't find a repo to put the auto-instance in (as you have no repos).

If everything otherwise looks good, make a fresh repo using the DVID command line (or the API). Wait a minute for things to settle, then stop DVID, and start it back up again.

Again keep a look at the logs - if things go well, this time DVID will find the (only) repo and create the data instances within it, one for every scale present in the precomputed bucket. These will also show up in the DVID webUI and from the informational API endpoints in DVID.

Testing
====
At this point you are ready to use whatever DVID clients you normally use to test the repo (most likely NeuTu).

Caveats
====
Not all of DVID's endpoints are supported by DVID operating in this mode. /specificblocks works. Most other endpoints do not.

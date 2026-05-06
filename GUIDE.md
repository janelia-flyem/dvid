User Guide
==========

For the detailed user guide, please see the [DVID Wiki][wiki].

[wiki]: https://github.com/janelia-flyem/dvid/wiki


Installation
============

Pre-built binary distributions (with all necessary dependencies)
are provided on the [github releases downloads page][1].

[1]: https://github.com/janelia-flyem/dvid/releases

Alternatively, [conda](https://conda.io/docs) users can simply install the `dvid` package:

Install:

```
$ conda create -n dvidenv -c flyem-forge -c conda-forge dvid
```

Run:

```
$ conda activate dvidenv
$ dvid about
```


Developer Guide
===============

Setup
-----

DVID is now built as a normal Go module. You do not need to put the checkout
under `GOPATH`.

The default developer build includes the storage backends used for modern DVID
deployments:

```
badger filestore ngprecomputed
```

The default build does not include the legacy `basholeveldb` backend.

Required for the default build:

```
Go 1.25 or newer
CGO enabled
a working C compiler
python3, or set PYTHON=/path/to/python when running make
```

On Ubuntu, the system prerequisites are typically:

```
$ sudo apt-get install build-essential bzip2 python3
```

On macOS, install Xcode Command Line Tools or an equivalent C compiler.

Clone the repository anywhere:

```
$ git clone https://github.com/janelia-flyem/dvid
$ cd dvid
```

No setup script is required if those tools are already available on your
system.

If you prefer conda to provide Go and the compiler toolchain, create and
activate a non-base environment, then run the optional conda toolchain script:

```
$ conda create -y -n dvid-devel
$ conda activate dvid-devel
$ ./scripts/install-conda-dev-toolchain.sh
$ conda activate dvid-devel
```

The final `conda activate` refreshes compiler-related environment variables
installed by conda packages.


Build and Test
--------------

Build the default DVID server:

```
$ make dvid
$ bin/dvid about
```

Run the standard test target:

```
$ make test
```

Install the default command-line tools:

```
$ make install
```

If a conda environment is active, `make install` installs into
`${CONDA_PREFIX}`. Otherwise it installs into `/usr/local`. Override the
destination with `INSTALL_PREFIX`:

```
$ make install INSTALL_PREFIX=/path/to/install-prefix
```


Legacy Basho LevelDB Build
--------------------------

As of May 2024, the [Basho LevelDB backend](https://github.com/basho/leveldb)
is no longer part of the default build because the project is not actively
maintained and it adds native library requirements. New DVID repositories should
use Badger for local embedded key-value storage.

Build Basho LevelDB support only when you need to open or migrate older DVID
repositories whose TOML configuration uses `basholeveldb`.

With conda:

```
$ conda activate dvid-devel
$ ./scripts/install-basholeveldb-dependencies.sh
$ make dvid-basholeveldb
$ bin/dvid-basholeveldb about
```

The `dvid-basholeveldb` target builds a separate binary at
`bin/dvid-basholeveldb` using:

```
badger basholeveldb filestore ngprecomputed
```

If you manage native dependencies outside conda, make sure the LevelDB C header
and library are visible to CGO, then build with:

```
$ DVID_BACKENDS="badger basholeveldb filestore ngprecomputed" make dvid
```


Releases
--------

For each platform (Mac and Linux):

1. Fetch the latest tags:

    ```
    $ git fetch --tags origin
    ```

   Or make your own release tag:

    ```
    $ git tag -a 'v0.8.20' -m "This is release v0.8.20"
    $ git push --tags origin
    ```

2. Build the conda package and upload it to the `flyem-forge` channel on `http://anaconda.org`:

    ```
    $ conda activate base
    $ GOMAXPROCS=4 conda build scripts/conda-recipe
    $ anaconda upload -u flyem-forge $(conda info --base)/conda-bld/osx-64/dvid-someVersionPlusHash-0.tar.bz2 # Mac
    $ anaconda upload -u flyem-forge $(conda info --base)/conda-bld/linux-64/dvid-someVersionPlusHash-0.tar.bz2 # Linux
    ```
   (The exact name of the tarball to upload to Anaconda will be described by the script output.)

   **Note:** For maximum Linux compatibility, build within the [`flyem-build`][flyem-build] Docker container:
   
   <details>
   
   <summary>Click here to see Docker container commands</summary>
   
   **Note:** Make sure your Docker container has plenty of RAM, or DVID's test
   suite may encounter random unexplained failures.
   (Set at least 16GB in Docker Preferences > Advanced.)
   
   ```
   # Launch the container
   git clone https://github.com/janelia-flyem/flyem-build-container
   cd flyem-build-container
   ./launch.sh # (or resume.sh)


   # Within the container
   # Note: It's recommended to use GOMAXPROCS to limit the cpus used in the build/tests
   conda activate
   cd /flyem-workspace/gopath/src/github.com/janelia-flyem/dvid
   git pull
   git checkout <tag>
   GOMAXPROCS=4 conda build scripts/conda-recipe
   anaconda upload -u flyem-forge /opt/conda/conda-bld/linux-64/dvid-someVersion-someHash_0.tar.bz2
   scripts/make-release-distribution.sh someVersion
   ```
   
   You can move the generated tarball to the host from your container with the following:
   ```
   % docker cp flyem-build:/flyem-workspace/gopath/src/github.com/janelia-flyem/dvid/dvid-0.9.6-dist-linux.tar.bz2 .
   ```
   </details>

   **Note:** For maximum macOS compatibility, use a macOS SDK and deployment
   target supported by the Go version in `go.mod` and the conda-forge compiler
   stack. If `CONDA_BUILD_SYSROOT` or `MACOSX_DEPLOYMENT_TARGET` are set, the
   Makefile forwards them to CGO.

3. Generate a release distribution.
   (This doesn't build dvid again; it uses the conda package you uploaded in the previous step.)

    ```
    $ ./scripts/make-release-distribution.sh 0.9.10
    $ ls dvid-0.9.10-dist-mac.tar.bz2 # <--- Distribution tarball includes dvid and all dependencies
    ```

4. [Draft a GitHub release.][creating-releases]

[flyem-build]: https://github.com/janelia-flyem/flyem-build-container
[creating-releases]: https://help.github.com/articles/creating-releases


Build Maintenance Notes
-----------------------

- New compiled (C/C++) dependencies for the default build should be packaged
  for conda and uploaded to the `flyem-forge` channel, if they aren't already
  available on the `conda-forge` channel. Then list them in
  `scripts/conda-recipe/meta.yaml` and update
  `scripts/install-conda-dev-toolchain.sh`.

- Dependencies needed only for legacy Basho LevelDB builds belong in
  `scripts/install-basholeveldb-dependencies.sh`, not in the default conda
  recipe.

- Of course, new DVID sources should be listed in the `Makefile` as needed.


Memory profiling
----------------

DVID uses [an integrated memory profiling system](https://github.com/wblakecaldwell/profiler/tree/d0f7b0590a127b0c7ef1abf7c089ef2fa74b47cd).  To start memory profiling, visit `http://path-to-dvid-server/profiler/start`.  You can then visit `http://path-to-dvid-server/profiler/info.html` to view the real-time graph of memory usage.  Stop profiling by visiting `http://path-to-dvid-server/profiler/stop`.

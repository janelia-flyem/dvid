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
$ conda create -n dvidenv -c flyem-forge -c conda-forge/label/cf201901 dvid
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

1. If you are using a bare OS, you will need some essentials (gcc, bzip2, etc) before installing anything else.  In Ubuntu, you can easily install this with `sudo apt-get install build-essential gcc bzip2`.
Then Install `conda` (specifically the [miniconda distribution](https://docs.conda.io/en/latest/miniconda.html)) and enable the `activate` command for your shell.  (If you are installing conda >= 4.6, you'll need to run `conda init bash` or whatever depending on your shell.)

   **Note:** These developer instructions assume you are using conda 4.5 or greater.

2. Upgrade conda and install `conda-build` and `anaconda-client`:

    ```
    $ conda activate base
    $ conda install conda-build anaconda-client
    ```
    
    If you already had those installed, be sure to update to the latest versions:
    
    ```
    $ conda update -n base conda-build anaconda-client
    ```

3. Add `flyem-forge` and `conda-forge/label/cf201901` to your `.condarc` file:

    ```
     $ cat ~/.condarc
     channels:
     - flyem-forge
     - conda-forge/label/cf201901
     - defaults
    ```

4. Create a conda environment for dvid development.  Activate it.

    ```
    $ conda create -y -n dvid-devel
    $ conda activate dvid-devel
    ```

5. Define `GOPATH` and clone the dvid source code into the appropriate subdirectory:

    ```
    $ export GOPATH=/path/to/gopath-dir
    $ DVID_SRC=${GOPATH}/src/github.com/janelia-flyem/dvid
    $ git clone http://github.com/janelia-flyem/dvid ${DVID_SRC}
    ```

6. Install the developer dependencies

    ```
    $ cd ${DVID_SRC}
    $ ./scripts/install-developer-dependencies.sh
    ```


Build and Test
--------------

    $ make dvid

    $ make test
    
    $ make install # Optional: Install bin/dvid into dvid-devel environment


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
    $ conda build scripts/conda-recipe
    $ anaconda upload -u flyem-forge $(conda info --base)/conda-bld/osx-64/dvid-0.8.20-0.tar.bz2 # Mac
    $ anaconda upload -u flyem-forge $(conda info --base)/conda-bld/linux-64/dvid-0.8.20-0.tar.bz2 # Linux
    ```

   Note: For maximum Linux compatibility, build within the [`flyem-build`][flyem-build] Docker container:
   
   <details>
   
   <summary>Click here to see Docker container commands</summary>
   
   ```
   # Launch the container
   git clone https://github.com/janelia-flyem/flyem-build-container
   cd flyem-build-container
   ./launch.sh # (or resume.sh)


   # Within the container
   cd /flyem-workspace/gopath/src/github.com/janelia-flyem/dvid
   conda build scripts/conda-recipe
   anaconda upload /opt/conda/conda-bld/linux-64/dvid-0.8.20-0.tar.bz2
   ```
   
   </details>

3. Generate a release distribution.
   (This doesn't build dvid again; it uses the conda package you uploaded in the previous step.)

    ```
    $ ./scripts/make-release-distribution.sh
    $ ls dvid-0.8.20-dist-mac.tar.bz2 # <--- Distribution tarball includes dvid and all dependencies
    ```

4. [Draft a GitHub release.][creating-releases]

[flyem-build]: https://github.com/janelia-flyem/flyem-build-container
[creating-releases]: https://help.github.com/articles/creating-releases


Build Maintenance Notes
-----------------------

- New compiled (C/C++) dependencies should be packaged for conda and uploaded
  to the `flyem-forge` channel, if they aren't already available on the 
  `conda-forge/label/cf201901` channel. Then list them in the `requirements` sections of
  `scripts/conda-recipe/meta.yaml`.  Also add them to the appropriate line of
  `scripts/install-developer-dependencies.sh`.

- New third-party Go dependencies can simply be added to `scripts/get-go-dependencies.sh`,
  but the conda recipe will build faster if you also add a corresponding entry
  to the `source` section in `scripts/conda-recipe/meta.yaml`.
     
- Of course, new DVID sources should be listed in the `Makefile` as needed.


Memory profiling
----------------

DVID uses [an integrated memory profiling system](https://github.com/wblakecaldwell/profiler/tree/d0f7b0590a127b0c7ef1abf7c089ef2fa74b47cd).  To start memory profiling, visit `http://path-to-dvid-server/profiler/start`.  You can then visit `http://path-to-dvid-server/profiler/info.html` to view the real-time graph of memory usage.  Stop profiling by visiting `http://path-to-dvid-server/profiler/stop`.

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

1. If you are using a bare OS, you will need some essentials (gcc, bzip2, etc) before installing anything else.  In Ubuntu, you can easily install this with `sudo apt-get install build-essential gcc bzip2`.
Then Install `conda` (specifically the [miniconda distribution][miniconda]) and enable the `activate` command for your shell.

    [miniconda]: https://docs.conda.io/en/latest/miniconda.html

    **Note:** At the time of this writing, the Miniconda installer will give you conda-4.5, but these instructions require conda-4.6.
    Immediately after installing Miniconda, upgrade `conda`:
   
    ```
    $ conda update -n base -c defaults conda
    ```
   
    ...and then activate it for your shell:
   
    ```
    $ conda init bash
    ```
   
    ...and then open a new terminal window.


2. Install (or upgrade) `conda-build` and `anaconda-client`:

    ```
    $ conda activate base
    $ conda install -n base -c defaults conda-build anaconda-client
    ```
    
3. Add `flyem-forge` and `conda-forge` to your `.condarc` file:

    ```
     $ cat ~/.condarc
     channels:
     - flyem-forge
     - conda-forge
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

7. Reactivate the environment

   **Important:** Yes, you must do this after `install-developer-dependencies.sh`, even though your `dvid-devel` environment is already active.
   Activating the environment again ensures that environment variables like `CC` and `CXX` are set correctly.

    ```
    $ conda activate dvid-devel
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

   **Note:** For maximum macOS compatibility, make sure you have the SDK for MacOSX 10.10,
   which can be installed via the [xcodelegacy project](https://github.com/devernay/xcodelegacy).

   <details>

   <summary>Click here for MacOSX 10.10 SDK installation commands</summary>

   ```
   curl https://raw.githubusercontent.com/devernay/xcodelegacy/master/XcodeLegacy.sh > XcodeLegacy.sh
   chmod +x XcodeLegacy.sh
   
   ./XcodeLegacy.sh -osx1010 buildpackages
   sudo ./XcodeLegacy.sh -osx1010 install
   ```

   **Note:** The XcodeLegacy script will probably tell you that the
   10.10 SDK can only be built/installed by an old version of Xcode,
   and ask you to download the old version (but not install it).
   
   Follow the download instructions it gives you, and then re-run
   `XcodeLegacy.sh` from the directory where you downloaded the Xcode
   `.dmg` file.
   
   (Also note that downloading Xcode requires an Apple Developer login.)

   </details>

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

- New compiled (C/C++) dependencies should be packaged for conda and uploaded
  to the `flyem-forge` channel, if they aren't already available on the 
  `conda-forge` channel. Then list them in the `requirements` sections of
  `scripts/conda-recipe/meta.yaml`.  Also add them to the appropriate line of
  `scripts/install-developer-dependencies.sh`.

- Of course, new DVID sources should be listed in the `Makefile` as needed.


Memory profiling
----------------

DVID uses [an integrated memory profiling system](https://github.com/wblakecaldwell/profiler/tree/d0f7b0590a127b0c7ef1abf7c089ef2fa74b47cd).  To start memory profiling, visit `http://path-to-dvid-server/profiler/start`.  You can then visit `http://path-to-dvid-server/profiler/info.html` to view the real-time graph of memory usage.  Stop profiling by visiting `http://path-to-dvid-server/profiler/stop`.

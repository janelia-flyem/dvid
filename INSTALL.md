DVID Installation
------------

Use the [conda][miniconda] package manager to install DVID:

```
## Install DVID
$ conda create -n dvidenv -c flyem-forge dvid

## Run
$ source activate dvidenv
$ dvid -help
```

[miniconda]: https://conda.io/miniconda.html


Developer Setup
---------------

1. Install [`conda`][miniconda].

2. Install `conda-build`:

     ```bash
     $ source activate root
     $ conda install conda-build
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

    ```bash
    $ conda create -n dvid-devel
    $ source activate dvid-devel
    ```

5. Define `GOPATH` and clone the dvid source code into the appropriate subdirectory:

    ```bash
    $ export GOPATH=/path/to/gopath-dir
    $ DVID_SRC=${GOPATH}/src/github.com/janelia-flyem/dvid
    $ git clone http://github.com/janelia-flyem/dvid ${DVID_SRC}
    ```

6. Install the developer dependencies

    ```
    $ cd ${DVID_SRC}
    $ ./scripts/install-developer-dependencies.sh
    ```

7. Build

    ```
    $ make dvid
    ```

8. Test

    ```
    $ make test
    ```

9. Tag a release; build the conda package:

    ```
    $ git tag -a 'v0.8.20' -m "This is release v0.8.20"
    $ git push --tags origin
    $ conda build scripts/conda-recipe
    ```

10. Maintain the build:

   - New compiled (C/C++) dependencies should be packaged for conda and uploaded
     to the `flyem-forge` channel, if they aren't already available on the 
     `conda-forge` channel. Then list them in the `requirements` sections of
     `scripts/conda-recipe/meta.yaml`.

   - New third-party Go dependencies can simply be added to `scripts/get-go-dependencies.sh`,
     but the conda recipe will build faster if you also add a corresponding entry
     to the `source` section in `scripts/conda-recipe/meta.yaml`.
     
   - Of course, new DVID sources should be listed in the `Makefile` as needed.

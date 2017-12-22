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
$ conda create -n dvidenv -c flyem-forge dvid
```

Run:

```
$ source activate dvidenv
$ dvid about
```


Developer Guide
===============

Setup
-----

1. Install [`conda`][miniconda].

[miniconda]: https://conda.io/miniconda.html

2. Install `conda-build`:

    ```
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

    ```
    $ conda create -n dvid-devel
    $ source activate dvid-devel
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


Releases
--------

For each platform (Mac and Linux):

1. Tag a release; build the conda package:

    ```
    $ git tag -a 'v0.8.20' -m "This is release v0.8.20"
    $ git push --tags origin
    $ conda build scripts/conda-recipe
    ```

2. Generate a release distribution:

    ```
    $ ./scripts/make-release-distribution.sh
    ```

Then [draft a GitHub release.][creating-releases]

[creating-releases]: https://help.github.com/articles/creating-releases


Build Maintenance Notes
-----------------------

- New compiled (C/C++) dependencies should be packaged for conda and uploaded
  to the `flyem-forge` channel, if they aren't already available on the 
  `conda-forge` channel. Then list them in the `requirements` sections of
  `scripts/conda-recipe/meta.yaml`.

- New third-party Go dependencies can simply be added to `scripts/get-go-dependencies.sh`,
  but the conda recipe will build faster if you also add a corresponding entry
  to the `source` section in `scripts/conda-recipe/meta.yaml`.
     
- Of course, new DVID sources should be listed in the `Makefile` as needed.

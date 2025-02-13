# Ceph - a scalable distributed storage system

Please see http://ceph.com/ for current info.


## Contributing Code

Most of Ceph is dual licensed under the LGPL version 2.1 or 3.0.  Some
miscellaneous code is under BSD-style license or is public domain.
The documentation is licensed under Creative Commons
Attribution Share Alike 3.0 (CC-BY-SA-3.0).  There are a handful of headers
included here that are licensed under the GPL.  Please see the file
COPYING for a full inventory of licenses by file.

Code contributions must include a valid "Signed-off-by" acknowledging
the license for the modified or contributed file.  Please see the file
SubmittingPatches.rst for details on what that means and on how to
generate and submit patches.

We do not require assignment of copyright to contribute code; code is
contributed under the terms of the applicable license.


## Checking out the source

You can clone from github with

	git clone git@github.com:ceph/ceph

or, if you are not a github user,

	git clone git://github.com/ceph/ceph

Ceph contains many git submodules that need to be checked out with

	git submodule update --init --recursive


## Build Prerequisites

The list of Debian or RPM packages dependencies can be installed with:

	./install-deps.sh


## Building Ceph

Note that these instructions are meant for developers who are
compiling the code for development and testing.  To build binaries
suitable for installation we recommend you build deb or rpm packages,
or refer to the `ceph.spec.in` or `debian/rules` to see which
configuration options are specified for production builds.

Build instructions:

	./do_cmake.sh
	cd build
	make

(Note: do_cmake.sh now defaults to creating a debug build of ceph that can
be up to 5x slower with some workloads. Please pass 
"-DCMAKE_BUILD_TYPE=RelWithDebInfo" to do_cmake.sh to create a non-debug
release.)

(Note: `make` alone will use only one CPU thread, this could take a while. use
the `-j` option to use more threads. Something like `make -j$(nproc)` would be
a good start.

This assumes you make your build dir a subdirectory of the ceph.git
checkout. If you put it elsewhere, just point `CEPH_GIT_DIR`to the correct
path to the checkout. Any additional CMake args can be specified setting ARGS
before invoking do_cmake. See [cmake options](#cmake-options)
for more details. Eg.

    ARGS="-DCMAKE_C_COMPILER=gcc-7" ./do_cmake.sh

To build only certain targets use:

	make [target name]

To install:

	make install
 
### CMake Options

If you run the `cmake` command by hand, there are many options you can
set with "-D". For example the option to build the RADOS Gateway is
defaulted to ON. To build without the RADOS Gateway:

	cmake -DWITH_RADOSGW=OFF [path to top level ceph directory]

Another example below is building with debugging and alternate locations 
for a couple of external dependencies:

	cmake -DLEVELDB_PREFIX="/opt/hyperleveldb" \
	-DCMAKE_INSTALL_PREFIX=/opt/ceph -DCMAKE_C_FLAGS="-O0 -g3 -gdwarf-4" \
	..

To view an exhaustive list of -D options, you can invoke `cmake` with:

	cmake -LH

If you often pipe `make` to `less` and would like to maintain the
diagnostic colors for errors and warnings (and if your compiler
supports it), you can invoke `cmake` with:

	cmake -DDIAGNOSTICS_COLOR=always ..

Then you'll get the diagnostic colors when you execute:

	make | less -R

Other available values for 'DIAGNOSTICS_COLOR' are 'auto' (default) and
'never'.


## Building a source tarball

To build a complete source tarball with everything needed to build from
source and/or build a (deb or rpm) package, run

	./make-dist

This will create a tarball like ceph-$version.tar.bz2 from git.
(Ensure that any changes you want to include in your working directory
are committed to git.)


## Running a test cluster

To run a functional test cluster,

	cd build
	make vstart        # builds just enough to run vstart
	../src/vstart.sh --debug --new -x --localhost --bluestore
	./bin/ceph -s

Almost all of the usual commands are available in the bin/ directory.
For example,

	./bin/rados -p rbd bench 30 write
	./bin/rbd create foo --size 1000

To shut down the test cluster,

	../src/stop.sh

To start or stop individual daemons, the sysvinit script can be used:

	./bin/init-ceph restart osd.0
	./bin/init-ceph stop


## Running unit tests

To build and run all tests (in parallel using all processors), use `ctest`:

	cd build
	make
	ctest -j$(nproc)

(Note: Many targets built from src/test are not run using `ctest`.
Targets starting with "unittest" are run in `make check` and thus can
be run with `ctest`. Targets starting with "ceph_test" can not, and should
be run by hand.)

When failures occur, look in build/Testing/Temporary for logs.

To build and run all tests and their dependencies without other
unnecessary targets in Ceph:

	cd build
	make check -j$(nproc)

To run an individual test manually, run `ctest` with -R (regex matching):

	ctest -R [regex matching test name(s)]

(Note: `ctest` does not build the test it's running or the dependencies needed
to run it)

To run an individual test manually and see all the tests output, run
`ctest` with the -V (verbose) flag:

	ctest -V -R [regex matching test name(s)]

To run an tests manually and run the jobs in parallel, run `ctest` with 
the `-j` flag:

	ctest -j [number of jobs]

There are many other flags you can give `ctest` for better control
over manual test execution. To view these options run:

	man ctest


## Building the Documentation

### Prerequisites

The list of package dependencies for building the documentation can be
found in `doc_deps.deb.txt`:

	sudo apt-get install `cat doc_deps.deb.txt`

### Building the Documentation

To build the documentation, ensure that you are in the top-level
`/ceph` directory, and execute the build script. For example:

	admin/build-doc


## `dfork` manual

See https://docs.ceph.com/en/latest/man/8/rbd/ for the general `rbd` man page

### New dfork with improved COW performance
This version of dfork does not create a separate disk image, but instead map the
same image with a switch to turn on/off dfork mode. 

#### Switch on/off dfork mode for a disk image

	rbd dfork switch foo --on/off        # switch on/off for foo
	rbd dfork switch --off --all         # switch off for all images

### Old version with snapshotting/cloning
This version relies on Ceph's snapshotting and cloning mechanisms. 

#### Create/remove a dfork-ed disk image

	rbd dfork create|add foo@dfoo        # dfoo = dfork(foo)
	rbd dfork remove|rm  foo@dfoo        # remove the dfork-ed disk dfoo

For creating a dfork-ed disk, there will be a intermediate snapshot named 
`foo-snap` associated with the parent image `foo`. There must *not* already be 
an image/snapshot named `foo-snap` for dfork create to succeed.

#### Check the dirty bit

	rbd dfork dirty foo 		# returns the dirty bit value of image foo
	rbd dfork dirty foo --block-on-clean 	# block writes to foo if clean

This API supports the abort-dfork() fault-tolerance approach, with the 
`--block-on-clean` providing guarantee for atomic switchover. 

#### Dirty bit actions (for internal use)

	rbd dfork __dirty foo --set|--clear 	# set/clear the dirty bit of foo
	rbd dfork __dirty foo --reset 			# reset the dirty bit

Set or clear the dirty bit of a given image. This API is not supposed to 
be exposed to clients but only for development use.

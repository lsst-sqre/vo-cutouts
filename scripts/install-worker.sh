#!/bin/bash

# This script updates and installs the necessary prerequisites for an image
# cutout backend starting with a stack container.

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for details.
# set -u is omitted because the setup bash function does not support it.
set -eo pipefail

# Enable the stack.  This should be done before set -x because it will
# otherwise spew a bunch of nonsense no one cares about into the logs.
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib

# Display each command as it's run.
set -x

# Download the image cutout backend.  This can be removed if RFC-828 is
# implemented, since that will include the image cutout backend in
# lsst-distrib.
#
# Currently, this uses the main branch because there appears to be no
# alternative (no releases and no tags).
mkdir /backend
cd /backend
git clone --depth 1 https://github.com/lsst-dm/image_cutout_backend.git
cd image_cutout_backend
setup -r .
scons install declare -t current

# Install Python dependencies.
pip install --no-cache-dir 'dramatiq[redis]' safir structlog

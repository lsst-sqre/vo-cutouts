#!/bin/bash

# This script is installed in the worker image and starts the cutout backend
# worker using arq. It must run with the stack environment configured and the
# image cutout backend imported.

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for details.
# set -u is omitted because the setup bash function does not support it.
set -eo pipefail

# Initialize the environment.
source /opt/lsst/software/stack/loadLSST.bash
setup lsst_distrib
setup image_cutout_backend

# Start Dramatiq with the worker.
arq vocutouts.workers.cutout.WorkerSettings

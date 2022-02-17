#!/bin/bash

# Upgrade the CentOS packages in worker images.  This is done in a separate
# script to create a separate cached Docker image, which will help with
# iteration speed on the more interesting setup actions taken later in the
# build.

# Bash "strict mode", to help catch problems and bugs in the shell
# script. Every bash script you write should include this. See
# http://redsymbol.net/articles/unofficial-bash-strict-mode/ for details.
set -euo pipefail

# Display each command as it's run.
set -x

# Upgrade the Red Hat packages.
yum -y upgrade
yum clean all

#! /bin/bash
#
# Given a bucket containing coverage.py data, this script retrieves and combines
# that coverage data, generates an HTML report, and pushes the results back to
# the bucket. A textual report is also printed for convenience when looking at
# the CI.
#
# This script assumes that the provided bucket has a folder inside it with a
# name corresponding to the current gpdb_src commit SHA.
#
set -ex

if [ $# -ne 1 ]; then
    echo "Usage: $0 COVERAGE_BUCKET_URI"
    exit 1
fi

BUCKET="$1"
CWD=$(pwd)
read -r COMMIT_SHA < gpdb_src/.git/HEAD

# Coverage.py needs to be able to find the source files that were used during
# the coverage run. The easiest way to do that for the majority of those files
# is to install GPDB in the same place it was installed on the clusters.
source ./gpdb_src/concourse/scripts/common.bash
time install_gpdb

pip install awscli coverage

# Pull down the coverage data for our current commit.
mkdir ./coverage
aws s3 sync "$BUCKET" ./coverage --exclude '*' --include "$COMMIT_SHA/*"

cd "./coverage/$COMMIT_SHA"

# Installing GPDB gets most of the source we need, but Python sources that were
# inside the Git repo when they executed will be in a different location on this
# machine compared to the test clusters. Here, we use [paths] to tell
# coverage.py that any source files under /home/*/gpdb_src on the clusters can
# be found in our local copy of gpdb_src.
cat > .coveragerc <<EOF
[paths]
source =
    $CWD/gpdb_src
    /home/*/gpdb_src
EOF

# Now combine the individual coverage data files for analysis. There can be
# thousands of coverage files across an entire CI run, so we use a find | xargs
# pipeline to avoid execution limits.
find . -name '*.coverage.*' -print0 | xargs -0 coverage combine --append

# Generate an HTML report and sync it back to the bucket, then print out a quick
# text report for developers perusing the CI directly.
# XXX remove both -i's below once Python versions are fixed
coverage html -i -d .
aws s3 sync .. "$BUCKET"
coverage report -i

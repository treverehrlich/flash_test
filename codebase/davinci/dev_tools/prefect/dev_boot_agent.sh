#! /usr/bin/env bash

# This file actually boots the Prefect agent in
# a loop fashion. Whenever the *_specs.sh file
# terminates, it is relaunched throught this script

until ~/codebase/davinci/dev_tools/prefect/dev_specs.sh; do
        echo "Server died with exit code $?. Rebooting..." >&2
        sleep 1
done
#! /usr/bin/env bash

# This file defines how the Prefect agent boots up
echo "Activating conda env..."
source ~/anaconda3/etc/profile.d/conda.sh
conda activate prefect
echo "Booting up Prefect agent..."
nohup prefect agent start -q 'dev' > prefect_dev_log.log
echo "Agent crashed..."
exit 1
#! /usr/bin/env bash

source ~/anaconda3/etc/profile.d/conda.sh
conda activate test
cd ~/codebase/davinci/dev_tools/api_warmer/
python3 api_warmer.py
#!/bin/bash

PROJECT_DIR="$(dirname ${0%/*})"
cd $PROJECT_DIR
source bin/activate
cd airdelay
python import.py
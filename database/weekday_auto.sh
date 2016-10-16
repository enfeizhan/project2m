#!/bin/bash
cd ${DBCODEPATH}
${PYTHONPATH}/python cli.py auto --price-type=sector
${PYTHONPATH}/python cli.py auto --price-type=share

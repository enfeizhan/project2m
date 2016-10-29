#!/bin/bash
cd ~/Documents/studies/project2m_dev
/home/fei/miniconda3/envs/project2m/bin/python cli.py auto --price-type=sector
/home/fei/miniconda3/envs/project2m/bin/python cli.py auto --price-type=share
/home/fei/miniconda3/envs/project2m/bin/python cli.py pre-sentiment

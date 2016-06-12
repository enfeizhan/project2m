#!/bin/bash
cd /home/fei/Documents/studies/Project2M
source /home/fei/miniconda3/bin/activate Project2M 
# run routine file
python /home/fei/Documents/studies/Project2M/daily_updating.py
source /home/fei/miniconda3/bin/activate python2
# send notification emails
cd /home/fei/Documents/studies/Project2M/send_email
python /home/fei/Documents/studies/Project2M/send_email/gmail_api.py
source /home/fei/miniconda3/bin/deactivate

#!/bin/bash
cd ~/Documents/studies/Project2M
source ~/miniconda3/bin/activate project2m 
# run routine file
python bulk_update.py sector auto
python bulk_update.py share auto
source ~/miniconda3/bin/deactivate
# source /home/fei/miniconda3/bin/activate python2
# # send notification emails
# cd /home/fei/Documents/studies/Project2M/send_email
# python /home/fei/Documents/studies/Project2M/send_email/gmail_api.py
# source /home/fei/miniconda3/bin/deactivate

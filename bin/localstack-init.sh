#!/bin/sh
echo "Initializing local S3 bucket..."
awslocal s3 mb s3://score-set-csv-uploads-dev
echo "S3 bucket 'score-set-csv-uploads-dev' created."
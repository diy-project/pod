#!/bin/bash

if [ ! -f proxy.zip ]; then
    echo 'ERROR: proxy.zip not found. Please run ./collect.sh first.'
    exit 1
fi

echo -n "Enter the ARN of IAM role configured for Lambda, S3 and SQS: "
read ROLE_NAME

echo -n "Enter the name of the region to deploy to (e.g., 'us-west-2'): "
read REGION_NAME

echo -n "Enter the name of the function to create (e.g. 'proxy'): "
read FUNCTION_NAME

aws lambda create-function \
    --function-name $FUNCTION_NAME \
    --zip-file fileb://proxy.zip \
    --runtime python2.7 \
    --region $REGION_NAME \
    --role $ROLE_NAME \
    --handler proxy.handler \
    --memory-size 128 \
    --timeout 30

aws lambda invoke --invocation-type RequestResponse \
    --function-name $FUNCTION_NAME \
    --region $REGION_NAME \
    --log-type Tail \
    --payload '{"url":"http://google.com/","method":"GET","headers":{}}' \
    /dev/stdout

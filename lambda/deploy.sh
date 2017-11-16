#!/bin/bash

if [ ! -f proxy.zip ]; then
    echo 'ERROR: proxy.zip not found. Please run ./collect.sh first.'
    exit 1
fi

read -e -p "Enter the name of the function: " -i 'proxy' FUNCTION_NAME

read -e -p "Enter the name of the region to deploy to: " -i 'us-west-1' REGION_NAME

read -p "Update existing function? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    aws lambda update-function-code \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://proxy.zip
else
    echo -n "Enter the ARN of IAM role configured for Lambda, S3 and SQS: "
    read ROLE_NAME

    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --zip-file fileb://proxy.zip \
        --runtime python2.7 \
        --region $REGION_NAME \
        --role $ROLE_NAME \
        --handler proxy.handler \
        --memory-size 128 \
        --timeout 30
fi

echo 'Testing:' $FUNCTION_NAME
aws lambda invoke --invocation-type RequestResponse \
    --function-name $FUNCTION_NAME \
    --region $REGION_NAME \
    --log-type Tail \
    --payload '{"url":"http://google.com/","method":"GET","headers":{}}' \
    /dev/stdout

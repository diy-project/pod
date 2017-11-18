#!/bin/bash

LAMBDA_SIZE=256
LAMBDA_TIMEOUT=30

if [ ! -f proxy.zip ]; then
    echo "ERROR: proxy.zip not found. Please run ./collect.sh first."
    exit 1
fi

LAMBDA_PRIV_KEY_FILE="../lambda.private.txt" 
if [ ! -f $LAMBDA_PRIV_KEY_FILE ]; then
    echo "ERROR: $LAMBDA_PRIV_KEY_FILE not found. Please run gen_rsa_kp.py first."
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

    # TODO: should set the private key in a more secure manner
    aws lambda update-function-configuration \
        --function-name $FUNCTION_NAME \
        --environment "{\"Variables\":{\"RSA_PRIVATE_KEY\":\"$(cat $LAMBDA_PRIV_KEY_FILE)\"}}" \
        --memory-size $LAMBDA_SIZE \
        --timeout $LAMBDA_TIMEOUT
else
    echo -n "Enter the ARN of IAM role configured for Lambda, S3 and SQS: "
    read ROLE_NAME

    # TODO: should set the private key in a more secure manner
    aws lambda create-function \
        --function-name $FUNCTION_NAME \
        --environment "{\"Variables\":{\"RSA_PRIVATE_KEY\":\"$(cat $LAMBDA_PRIV_KEY_FILE)\"}}" \
        --zip-file fileb://proxy.zip \
        --runtime python2.7 \
        --region $REGION_NAME \
        --role $ROLE_NAME \
        --handler proxy.handler \
        --memory-size $LAMBDA_SIZE \
        --timeout $LAMBDA_TIMEOUT
fi

echo "Testing:" $FUNCTION_NAME
aws lambda invoke --invocation-type RequestResponse \
    --function-name $FUNCTION_NAME \
    --region $REGION_NAME \
    --log-type Tail \
    --payload '{"url":"http://google.com/","method":"GET","headers":{}}' \
    /dev/stdout

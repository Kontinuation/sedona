#!/bin/bash

# Set system environment variable
export WHEROBOTS_USERID="dummy@test.com"
export WHEROBOTS_AWS_ACCESSKEY="REPLACE_ME"
export WHEROBOTS_AWS_SECRETKEY="REPLACE_ME"
export WHEROBOTS_AWS_REGION="us-west-2"
export WHEROBOTS_AWS_S3BUCKET="test-wherobots-user-logs"

# Verify that the variable has been set
echo "WHEROBOTS_USERID is set to: $WHEROBOTS_USERID"
echo "WHEROBOTS_AWS_ACCESSKEY is set to: $WHEROBOTS_AWS_ACCESSKEY"
echo "WHEROBOTS_AWS_SECRETKEY is set to: $WHEROBOTS_AWS_SECRETKEY"
echo "WHEROBOTS_AWS_REGION is set to: $WHEROBOTS_AWS_REGION"
echo "WHEROBOTS_AWS_S3BUCKET is set to: $WHEROBOTS_AWS_S3BUCKET"

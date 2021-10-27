#!/bin/sh
for ARGUMENT in "$@"
do
    KEY=$(echo $ARGUMENT | cut -f1 -d=)
    VALUE=$(echo $ARGUMENT | cut -f2 -d=)   

    case "$KEY" in
            stack_name)  stack_name=${VALUE} ;;
            *)   
    esac    

done

#move files to s3
aws s3api create-bucket --bucket $stack_name-code 
aws s3api put-object --bucket $stack_name-code --key spark-job.py  --body auxiliary/spark-job.py
aws s3api put-object --bucket $stack_name-code --key items.csv  --body auxiliary/items.csv
aws s3api put-object --bucket $stack_name-code --key install_boto3.sh --body auxiliary/install_boto3.sh
aws s3api put-object --bucket $stack_name-code --key generator.py --body auxiliary/generator.py
aws s3api put-object --bucket $stack_name-code --key requirements.txt --body auxiliary/requirements.txt

#run cloudformation script
aws cloudformation create-stack --stack-name $stack_name --template-body file://cloudformation.yaml
aws cloudformation wait stack-create-complete --stack-name $stack_name
aws cloudformation update-stack --stack-name $stack_name --template-body file://cloudformation_update.yaml



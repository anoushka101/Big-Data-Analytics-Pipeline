import boto3
import pandas as pd
import io
import os
import json

# Initialize AWS clients
s3 = boto3.client('s3')
sns = boto3.client('sns')

# Environment variables
DEST_BUCKET = 's3://processed-data-anoushka/'  # Destination bucket for processed data
SNS_TOPIC_ARN ='arn:aws:sns:eu-north-1:741448918330:data-pipeline-status'  # SNS Topic ARN for notifications

def lambda_handler(event, context):
    try:
        # Extract the S3 bucket name and file key from the S3 event
        source_bucket = event['Records'][0]['s3']['bucket']['name']
        file_key = event['Records'][0]['s3']['object']['key']
        
        print(f"Processing file: {file_key} from bucket: {source_bucket}")

        # Step 1: Read the file from the source S3 bucket
        response = s3.get_object(Bucket=source_bucket, Key=file_key)
        data = response['Body'].read()
        
        # Step 2: Load the file content into a Pandas DataFrame
        df = pd.read_csv(io.BytesIO(data))

        print("Original DataFrame:")
        print(df.head())

        # Step 3: Data Processing
        # a. Drop rows with any null values
        df_cleaned = df.dropna()

        # b. Handle outliers: Remove rows where any numeric column has values > 99th percentile
        numeric_columns = df_cleaned.select_dtypes(include=['number']).columns
        for col in numeric_columns:
            q99 = df_cleaned[col].quantile(0.99)
            df_cleaned = df_cleaned[df_cleaned[col] <= q99]

        print("Processed DataFrame:")
        print(df_cleaned.head())

        # Step 4: Write the processed data back to the destination S3 bucket
        processed_file_key = f"processed/{file_key}"
        csv_buffer = io.StringIO()
        df_cleaned.to_csv(csv_buffer, index=False)

        s3.put_object(Bucket=DEST_BUCKET, Key=processed_file_key, Body=csv_buffer.getvalue())
        print(f"Processed file saved to {DEST_BUCKET}/{processed_file_key}")

        # Step 5: Send success notification via SNS
        success_message = (
            f"Data processing succeeded for file: {file_key}\n"
            f"Processed file saved at: s3://{DEST_BUCKET}/{processed_file_key}"
        )
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=success_message, Subject="Data Processing Successful")
        print("Success notification sent.")

        return {
            'statusCode': 200,
            'body': json.dumps('Processing complete.')
        }
    
    except Exception as e:
        # Handle errors and send failure notification
        error_message = f"Error processing file {file_key}: {str(e)}"
        sns.publish(TopicArn=SNS_TOPIC_ARN, Message=error_message, Subject="Data Processing Failed")
        print(f"Error: {error_message}")

        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

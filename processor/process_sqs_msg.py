import sys
sys.path.append("C:\\Users\\Harinath Reddy\\AppData\\Local\\Programs\\Python\\Python312\\Lib\\site-packages")

import boto3
import json
import pymysql # type: ignore

# Initialize SQS client
sqs = boto3.client('sqs', region_name='ap-south-1')
queue_url = 'https://sqs.ap-south-1.amazonaws.com/398042053881/demo-sqs'

# Initialize RDS connection
rds_host = "database-1.ct60k4kwchh8.ap-south-1.rds.amazonaws.com"
db_username = "admin"
db_password = "12345678"
db_name = "customer_info"
rds = pymysql.connect(
    host=rds_host,
    user=db_username,
    password=db_password,
    database=db_name,
    connect_timeout=5
)
def process_messages():
    # Receive messages from SQS
    response = sqs.receive_message(
        QueueUrl=queue_url,
        MaxNumberOfMessages=10  # Adjust as needed
    )
    
    messages = response.get('Messages', [])
    
    if not messages:
        print("No messages in the queue.")
        return
    
    for message in messages:
        try:
            body = json.loads(message['Body'])
            first_name = body.get("first_name")
            last_name = body.get("last_name")
            email = body.get("email")

            # Insert data into RDS database
            with rds.cursor() as cursor:
                sql = "INSERT INTO contacts (first_name, last_name, email) VALUES (%s, %s, %s)"
                cursor.execute(sql, (first_name, last_name, email))
                rds.commit()

            # Delete message from SQS after processing
            sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message['ReceiptHandle']
            )
            print("Message processed and deleted from SQS.")
        except Exception as e:
            print(f"Error processing message: {e}")

if __name__ == "__main__":
    process_messages()

import boto3
import pymysql
import json
import os

QUEUE_URL = '*******'
s3_cient = boto3.client('s3')

def lambda_handler(event, context):
   
    rds_endpoint    =  os.environ.get("DB_HOST")
    username        =  os.environ.get("DB_USER") 
    password        =  os.environ.get("DB_PASSWORD")
    db_name         =  os.environ.get("DB_NAME")
    conn = None
    
    try:
        conn = pymysql.connect(host=rds_endpoint, user=username, passwd=password, db=db_name, connect_timeout=5)
    except pymysql.MySQLError as e:
        print("ERROR: Unexpected error: Could not connect to MySQL instance.")


    csv_file = event["Records"][0]["s3"]["object"]["key"]
    identifier = csv_file.split('__')
    
    campaignIndetifier = identifier[0]
    
    campaign = campaignIsSchedule(conn, campaignIndetifier)
    
    
    data = read_data_from_s3(event)
    updateSQL= "UPDATE campaigns SET total_contacts = {}  WHERE unique_identifier = '{}'".format(len(data), campaignIndetifier);
    conn.cursor().execute(updateSQL)
    
    # status = 1 processing start...
    changeCampaignStatus(conn, 1, campaignIndetifier)
    
    isSchedule = campaign[10]
    emailSubject = campaign[2]
    emailBody = campaign[3]

    with conn.cursor() as cur:
        for contact in data: # Iterate over S3 csv file content and insert into MySQL database
            try:
                contact = contact.replace("\n","").split(",")
               # print (">>>>>>>"+str(contact))
                if(len(contact) > 1):
                    sqlStr = "insert into contacts (name, email, status, campaign_indentifier) value('{}', '{}', {}, '{}')".format(contact[0], contact[1], contact[2], campaignIndetifier)
                    cur.execute(sqlStr)
                    conn.commit()
                    
                    if(isSchedule == 0 and contact[2] == '1'):
                        emailBody = emailBody.replace("{{name}}", contact[0])
                        sendingTOSQS(contact[1], emailSubject, emailBody )
    
            except pymysql.MySQLError as e:
                print(e)
                continue
    
    # status = 1 processing start...
    #If not schedule then don't update campaign status
    if(isSchedule == 0 ):
        changeCampaignStatus(conn, 2, campaignIndetifier)  
        
    if conn:
        conn.commit()



# Read CSV file content from S3 bucket
def read_data_from_s3(event):
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    s3_file_name = event["Records"][0]["s3"]["object"]["key"]
    resp = s3_cient.get_object(Bucket=bucket_name, Key=s3_file_name)
    data = resp['Body'].read().decode('utf-8')
    data = data.split("\n")
    
    if(len(data) > 0):
        data.pop(0)
 
    return data

def sendingTOSQS(email, subject, body):
    sqs = boto3.client('sqs')
    response = sqs.send_message(
        QueueUrl=QUEUE_URL,
        DelaySeconds=10,
        MessageAttributes={
            'Email': {
                'DataType': 'String',
                'StringValue': email
            },
            'Subject': {
                'DataType': 'String',
                'StringValue': subject
            }
        },
        MessageBody= body
    )
    
    #print(response)


def campaignIsSchedule(conn, identifier):
	campaignSQL = "SELECT * FROM campaigns WHERE unique_identifier ='{}' limit 1".format(identifier);
	print(campaignSQL)
	with conn.cursor() as cursor:    	
		cursor.execute(campaignSQL)
		row = cursor.fetchone()
	return row
	
	

def changeCampaignStatus(conn, status, campaignIndetifier):
    updateSQL= "UPDATE campaigns SET status = {}  WHERE unique_identifier = '{}'".format(status, campaignIndetifier);
    conn.cursor().execute(updateSQL)

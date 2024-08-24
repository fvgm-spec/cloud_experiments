## Introduction

We have something in common if you are a Developer, Data Engineer, or any person in the data world who loves to get the best performance of every process. Looking for prices of assets and values you want to invest could become a tedious task, what a regular person would do to get the BTC price of the day, would be do a Google search, but if you can do that in a daily basis, it is OK

[btc image]

## Use Case

Lastly I’ve had some clients who need to get some fresh data for assets they are investing in, BTC is one of them, so they have asked me to design a process where they could avoid the tedious step of looking for the data they need to know in a daily basis by making a google search, for them having the data automatically updated in a google sheet, would be a huge time saver.

To overcome that necessity, the first thing I thought of was developing a process that they don't need to trigger, instead, it can run automatically in the cloud, so the data they need would be ingested in their google sheets and they can review it and perform their analysis.

Due to the simplicity of the solution, I thought about some ingredients:

An API endpoint 
An HTTP trigger
A container where to store the data
A platform for analysis and visualization

Coindesk API endpoint

CoinDesk is one of the most commonly used sources to get data related to the global crypto economy. You can get Bitcoin prices by just doing a simple API call in your browser https://api.coindesk.com/v1/bpi/currentprice.json

[api_response]

We will design a simple infrastructure in AWS that retrieves Bitcoin (BTC) prices and then stores that data in JSON files in an S3 bucket. We will be using a combination of AWS services, primarily AWS Lambda, S3, and CloudWatch for scheduling. Below is an outline of the architecture and the steps needed to implement this solution.

[aws architechture]

Architecture Overview
Lambda Function: This will be the core of the architecture, responsible for:
Fetching the BTC price data from Coindeskl API.
Formatting the data as JSON.
Storing the JSON file in an S3 bucket.
S3 Bucket: This is where the JSON files containing the BTC price data will be stored.
CloudWatch Events (or EventBridge): This will trigger the Lambda function on a scheduled basis (e.g., every 5 minutes, hourly, etc.).
IAM Roles and Policies: Defines necessary permissions for Lambda to fetch data from the API and write to S3.
Step-by-Step Implementation
Step 1: Creating an empty S3 Bucket
This step can be completed through AWS CLI or the AWS Management Console. There are also some other multiple options like Terraform or AWS CloudFormation. By navigating through AWS Console to S3, you can create a new bucket with the name btc-price-data-bucket or any other you want.
[create s3]
Afterward, you’d need to configure the bucket to store the JSON files, and leave the rest of the settings as default.
Step 2: Create the Lambda Function
Go to Lambda in the AWS Management Console and click on Create function.
[create lambda]
Leave the selection as default in Author from scratch, and give your function a name (e.g., FetchBTCPrices).
Choose the Python 3.12 runtime.
[create lambda2]
Expand the option execution role in Change default execution role, and choose Create a new role with basic Lambda permissions. Then you can click the button Create function.
[lambda created]
You can test the code which is written by default in your Lambda function, by scrolling down to the Code source section, and then click in the button “Test”. Then you can replace that code with the one bellow, which will fetch BTC prices and store them in the S3 bucket.
Copy code
[Lambda code]
Remember that you must Delpoy 
[deploy changes]


Fff






To resolve this issue, you need to add an IAM policy to your Lambda function’s execution role that grants it permission to perform the s3:PutObject action on the specified S3 bucket.

Steps to Fix the Permissions Issue:
Identify the IAM Role: The error message includes the ARN of the role that needs permission: arn:aws:sts::806566178343:assumed-role/FetchBTCPrices-role-wl3f3d27/FetchBTCPrices.

Modify the IAM Role's Policy:

Go to the AWS IAM console.
In the left-hand menu, click Roles.
Search for the role FetchBTCPrices-role-wl3f3d27 (the role mentioned in the error).
Click on the role name to view its details.
Under the Permissions tab, look for the policy attached to this role. You may see an inline policy or managed policy.
Add S3 Permissions:

Click Add permissions -> Create inline policy.
Choose the JSON tab and enter a policy that grants the necessary S3 permissions. Here's an example policy:
json
Copy code
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": "s3:PutObject",
            "Resource": "arn:aws:s3:::btc-price-data-bucket/*"
        }
    ]
}
This policy allows the PutObject action on all objects within the btc-price-data-bucket.

Review and Apply Policy:

After adding the JSON policy, click Review policy.
Give the policy a name (e.g., S3PutObjectPolicy).
Click Create policy to apply the changes.
Test the Lambda Function:

Go back to your Lambda function and run a test to see if it can successfully write to the S3 bucket without encountering the access denied error.
Notes:
Least Privilege Principle: Ensure that you only grant the permissions necessary for your Lambda function. The example policy provided allows s3:PutObject access to all objects in the specified bucket. If you want to restrict access further (e.g., only specific folders or files), adjust the Resource field accordingly.
Additional Actions: If your Lambda function requires more S3 actions (like s3:ListBucket, s3:GetObject, etc.), make sure to include those in the policy as well.
Policy Propagation: IAM policy changes may take a few seconds to propagate. If the error persists immediately after updating the policy, wait a few seconds and try again.
By following these steps, your Lambda function should have the necessary permissions to write to the S3 bucket.







import boto3
boto3 = boto3.Session(profile_name="dipkDev")
""" session = boto3.Session(
    aws_access_key_id="YOUR_ACCESS_KEY",
    aws_secret_access_key="YOUR_SECRET_KEY",
    region_name="ap-south-1"
) 
s3 = session.client("s3")
"""

# Replace with your AWS region and bucket name
AWS_REGION = "ap-south-1"
BUCKET_NAME = "bucket-march2025-88c0f2dc566e4df5b5775bf2367576f6"
FOLDER_NAME = "data/"  # Folder names end with "/"

s3 = boto3.client("s3")


def list_s3_buckets():
    """
    List all S3 buckets in your AWS account.
    """
    s3 = boto3.client("s3")

    try:
        response = s3.list_buckets()
        buckets = response["Buckets"]

        if not buckets:
            print("No S3 buckets found.")
        else:
            print("S3 Buckets:")
            for bucket in buckets:
                print(f"- {bucket['Name']}")

    except Exception as e:
        print(f"Error listing S3 buckets: {e}")



def list_s3_objects(bucket_name, prefix=""):
    """
    List all files and folders within an S3 bucket.
    
    :param bucket_name: Name of the S3 bucket
    :param prefix: Folder path (optional, to list contents of a specific folder)
    """
    s3 = boto3.client("s3")
    
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if "Contents" not in response:
            print(f"No files found in bucket '{bucket_name}' with prefix '{prefix}'.")
            return
        
        print(f"Contents of S3 Bucket: {bucket_name}/{prefix}")
        
        folders = set()
        for obj in response["Contents"]:
            key = obj["Key"]
            if key.endswith("/"):  # Identify folders
                folders.add(key)
            else:
                print(f"File: {key}")

        # Print folders separately
        for folder in sorted(folders):
            print(f"Folder: {folder}")

    except Exception as e:
        print(f"Error listing S3 objects: {e}")

# Run the function
list_s3_buckets()
list_s3_objects(BUCKET_NAME)
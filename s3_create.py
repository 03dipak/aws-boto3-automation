import boto3
import uuid
boto3 = boto3.Session(profile_name="dipkDev")
import os
# Replace with your AWS region and bucket name
AWS_REGION = "ap-south-1"
#BUCKET_NAME = f"bucket-march2025-{uuid.uuid4().hex}"
BUCKET_NAME = "bucket-march2025-88c0f2dc566e4df5b5775bf2367576f6"
FOLDER_NAME = "data/"  # Folder names end with "/"

# Initialize S3 client
s3 = boto3.client("s3")

# Create S3 bucket (if not exists)
def create_bucket(bucket_name, region):
    try:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )
        print(f"Bucket '{bucket_name}' created successfully.")
    except s3.exceptions.BucketAlreadyOwnedByYou:
        print(f"Bucket '{bucket_name}' already exists and is owned by you.")
    except Exception as e:
        print(f"Error creating bucket: {e}")

# Create a folder (empty object with "/" suffix)
def create_folder(bucket_name, folder_name):
    s3.put_object(Bucket=bucket_name, Key=folder_name)
    print(f"Folder '{folder_name}' created in bucket '{bucket_name}'.")


def upload_folder_to_s3(folder_path, bucket_name, s3_folder):
    """
    Upload an entire local folder to an S3 bucket.

    :param folder_path: Local folder path
    :param bucket_name: Target S3 bucket name
    :param s3_folder: Destination folder in S3 (should end with '/')
    """
    s3 = boto3.client("s3")

    # Ensure S3 folder name ends with '/'
    if not s3_folder.endswith('/'):
        s3_folder += '/'

    # Walk through local folder and upload each file
    for root, _, files in os.walk(folder_path):
        for file in files:
            local_path = os.path.join(root, file)  # Full local path
            relative_path = os.path.relpath(local_path, folder_path)  # Relative path
            s3_key = s3_folder + relative_path.replace("\\", "/")  # S3 key

            try:
                s3.upload_file(local_path, bucket_name, s3_key)
                print(f"Uploaded {local_path} to s3://{bucket_name}/{s3_key}")
            except Exception as e:
                print(f"Error uploading {local_path}: {e}")


def delete_s3_folder(bucket_name, folder_name):
    """
    Delete all files inside a folder and remove the folder itself in an S3 bucket.

    :param bucket_name: Name of the S3 bucket
    :param folder_name: Folder path (must end with '/')
    """
    s3 = boto3.client("s3")

    # Ensure the folder name ends with '/'
    if not folder_name.endswith('/'):
        folder_name += '/'

    # List all objects in the folder
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)

    if "Contents" in objects:
        # Prepare objects for batch deletion
        delete_keys = [{"Key": obj["Key"]} for obj in objects["Contents"]]

        # Delete all files inside the folder
        s3.delete_objects(Bucket=bucket_name, Delete={"Objects": delete_keys})
        print(f"Deleted all files in folder: {folder_name}")

        # Delete the empty folder itself (optional)
        s3.delete_object(Bucket=bucket_name, Key=folder_name)
        print(f"Deleted empty folder: {folder_name}")
    else:
        print(f"Folder '{folder_name}' is already empty or does not exist.")

#create_bucket(BUCKET_NAME, AWS_REGION)
#delete_s3_folder(BUCKET_NAME, FOLDER_NAME)


#create_folder(BUCKET_NAME, FOLDER_NAME)


upload_folder_to_s3("data/", BUCKET_NAME, FOLDER_NAME)
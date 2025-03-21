import boto3
boto3 = boto3.Session(profile_name="dipkDev")
# Initialize Glue client
glue_client = boto3.client("glue", region_name="ap-south-1")  # Change to your AWS region
import boto3

# Initialize Boto3 clients
rds_client = boto3.client('rds')
glue_client = boto3.client('glue')
ec2_client = boto3.client('ec2')

# Define RDS instance identifier
RDS_INSTANCE_ID = "rds-mysql-instance"
GLUE_CONNECTION_NAME="my-rds-mysql-connection"
USERNAME = "admin"
PASSWORD = "vmsKuOY~nk38|^l#~" 
DATABASE = "MyDatabase"

# Step 1: Get RDS instance details
def get_rds_details(instance_id):
    response = rds_client.describe_db_instances(DBInstanceIdentifier=instance_id)
    db_instance = response['DBInstances'][0]

    # Get RDS status
    status = db_instance['DBInstanceStatus']
    
    # Get VPC ID
    vpc_id = db_instance['DBSubnetGroup']['VpcId']
    
    # Get Subnet IDs
    subnet_ids = [subnet['SubnetIdentifier'] for subnet in db_instance['DBSubnetGroup']['Subnets']]
    
    # Get Security Group IDs
    security_group_ids = [sg['VpcSecurityGroupId'] for sg in db_instance['VpcSecurityGroups']]
    
    # Get Availability Zone (AZ)
    availability_zone = db_instance['AvailabilityZone']
     # Get RDS Hostname (Endpoint)
    rds_hostname = db_instance['Endpoint']['Address']   
    return status, vpc_id, subnet_ids, security_group_ids,availability_zone,rds_hostname

# Step 2: Create a Glue Connection
def create_glue_connection(connection_name, subnet_id, security_group_ids,availability_zone,rds_hostname):
    try:
        response = glue_client.create_connection(
            ConnectionInput={
                'Name': connection_name,
                'ConnectionType': 'JDBC',
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": f"jdbc:mysql://{rds_hostname}:3306/{DATABASE}",
                    "USERNAME": USERNAME,
                    "PASSWORD": PASSWORD,  # Store in AWS Secrets Manager for better security
                    "JDBC_DRIVER_CLASS_NAME": "com.mysql.jdbc.Driver"
                },
                'PhysicalConnectionRequirements': {
                    'SubnetId': subnet_id,
                    'SecurityGroupIdList': security_group_ids,
                    "AvailabilityZone": availability_zone
                }
            }
        )
        return response
    except Exception as e:
        print(f"Error creating Glue connection: {e}")
        return None

def create_glue_connection_old(connection_name, host, database, username, password, subnet_id, security_group_id, availability_zone):
    """
    Creates an AWS Glue connection to an RDS MySQL instance.
    """
    try:
        response = glue_client.create_connection(
            ConnectionInput={
                "Name": connection_name,
                "Description": "Glue connection to MySQL RDS",
                "ConnectionType": "JDBC",
                "ConnectionProperties": {
                    "JDBC_CONNECTION_URL": f"jdbc:mysql://{host}:3306/{database}",
                    "USERNAME": username,
                    "PASSWORD": password,  # Store in AWS Secrets Manager for better security
                    "JDBC_DRIVER_CLASS_NAME": "com.mysql.jdbc.Driver"
                },
                "PhysicalConnectionRequirements": {
                    "SubnetId": subnet_id,
                    "SecurityGroupIdList": [security_group_id],
                    "AvailabilityZone": availability_zone
                }
            }
        )
        print(f"✅ Glue Connection '{connection_name}' created successfully!")
        return response
    except Exception as e:
        print(f"❌ Failed to create Glue Connection: {e}")

def delete_glue_connection(connection_name):
    """
    Deletes an AWS Glue connection by name.
    """
    try:
        response = glue_client.delete_connection(ConnectionName=connection_name)
        print(f"✅ Glue Connection '{connection_name}' deleted successfully!")
        return response
    except Exception as e:
        print(f"❌ Failed to delete Glue Connection: {e}")


def test_glue_connection(connection_name):
    """
    Tests an AWS Glue connection by retrieving its details.
    """
    try:
        response = glue_client.get_connection(Name=connection_name)
        print(f"✅ Glue Connection '{connection_name}' exists and is configured correctly!")
        return response
    except Exception as e:
        print(f"❌ Glue Connection '{connection_name}' test failed: {e}")


# Main Execution
if __name__ == "__main__":
    status, vpc_id, subnet_ids, security_group_ids,availability_zone,rds_hostname = get_rds_details(RDS_INSTANCE_ID)
    
    if status == "available":
        print(f"RDS is running. VPC: {vpc_id}, Subnets: {subnet_ids}, Security Groups: {security_group_ids}")
        
        # Select the first subnet (Modify if needed)
        delete_glue_connection(GLUE_CONNECTION_NAME)
        glue_connection_response = create_glue_connection(GLUE_CONNECTION_NAME, subnet_ids[0], security_group_ids,availability_zone,rds_hostname)
        
        if glue_connection_response:
            print("Glue Connection Created Successfully!")
            test_glue_connection(GLUE_CONNECTION_NAME)
        else:
            print("Failed to create Glue Connection.")
    else:
        print(f"RDS is not running. Current status: {status}")


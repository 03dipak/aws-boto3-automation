import boto3


def create_rds_instance(db_instance_id, master_username, master_password):
    """
    Create an Amazon RDS MySQL instance.

    :param db_instance_id: RDS instance identifier
    :param master_username: Master username
    :param master_password: Master password
    """
    rds = boto3.client("rds")

    try:
        response = rds.create_db_instance(
            DBInstanceIdentifier=db_instance_id,
            AllocatedStorage=20,  # 20GB storage
            DBInstanceClass="db.t3.micro",  # Free-tier instance type
            Engine="mysql",
            MasterUsername=master_username,
            MasterUserPassword=master_password,
            DBName="MyDatabase",
            PubliclyAccessible=True,  # Change to False if private
            BackupRetentionPeriod=7,  # Days to retain backups
            MultiAZ=False,  # Single AZ deployment
            StorageType="gp2"
        )

        print(f"RDS MySQL instance '{db_instance_id}' is being created.")
    except Exception as e:
        print(f"Error creating RDS instance: {e}")

def get_rds_endpoint(db_instance_id):
    """
    Get the endpoint (host) of an RDS instance.

    :param db_instance_id: RDS instance identifier
    """
    rds = boto3.client("rds")

    try:
        response = rds.describe_db_instances(DBInstanceIdentifier=db_instance_id)
        endpoint = response["DBInstances"][0]["Endpoint"]["Address"]
        print(f"RDS Endpoint: {endpoint}")
        return endpoint
    except Exception as e:
        print(f"Error retrieving endpoint: {e}")


def delete_rds_instance(db_instance_id):
    """
    Delete an RDS MySQL instance.

    :param db_instance_id: RDS instance identifier
    """
    rds = boto3.client("rds")

    try:
        response = rds.delete_db_instance(
            DBInstanceIdentifier=db_instance_id,
            SkipFinalSnapshot=True  # Change to False if you want a final snapshot
        )
        print(f"RDS MySQL instance '{db_instance_id}' is being deleted.")
    except Exception as e:
        print(f"Error deleting RDS instance: {e}")

def get_rds_details(instance_id):
    rds = boto3.client("rds")
    response = rds.describe_db_instances(DBInstanceIdentifier=instance_id)
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

        # Get Subnet and AZ mapping
    subnets_with_az = {subnet['SubnetIdentifier']: subnet['SubnetAvailabilityZone']['Name']
                       for subnet in db_instance['DBSubnetGroup']['Subnets']}
     # Get RDS Hostname (Endpoint)
    rds_hostname = db_instance['Endpoint']['Address']   
    return status, vpc_id, subnets_with_az, security_group_ids,availability_zone,rds_hostname


#get_rds_endpoint("rds-mysql-instance")
#print(get_rds_details("rds-mysql-instance"))
delete_rds_instance("rds-mysql-instance")

#create_rds_instance(db_instance_id="rds-mysql-instance", master_username="admin",  master_password="vmsKuOY~nk38|^l#~")


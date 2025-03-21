import boto3
boto3 = boto3.Session(profile_name="dipkDev")

def create_dynamodb_table(table_name):
    """
    Create a DynamoDB table with a primary key.

    :param table_name: Name of the DynamoDB table
    """
    dynamodb = boto3.client("dynamodb")

    try:
        response = dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {"AttributeName": "id", "KeyType": "HASH"}  # Partition key
            ],
            AttributeDefinitions=[
                {"AttributeName": "id", "AttributeType": "S"}  # String type
            ],
            ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}
        )

        print(f"DynamoDB table '{table_name}' is being created...")
        print(response)
    except Exception as e:
        print(f"Error creating table: {e}")


def insert_data_into_dynamodb(table_name, item):
    """
    Insert an item into a DynamoDB table.

    :param table_name: Name of the DynamoDB table
    :param item: Dictionary containing the item data
    """
    dynamodb = boto3.resource("dynamodb")
    table = dynamodb.Table(table_name)

    try:
        response = table.put_item(Item=item)
        print(f"Data inserted successfully: {item}")
    except Exception as e:
        print(f"Error inserting data: {e}")


def delete_dynamodb_table(table_name):
    """
    Delete a DynamoDB table.

    :param table_name: Name of the DynamoDB table to delete
    """
    dynamodb = boto3.client("dynamodb")

    try:
        response = dynamodb.delete_table(TableName=table_name)
        print(f"DynamoDB table '{table_name}' is being deleted...")
    except Exception as e:
        print(f"Error deleting table: {e}")



#create_dynamodb_table("customer_activity")
"""
insert_data_into_dynamodb(
    "customer_activity",
    {"id": "1", "name": "Alice", "age": 30, "city": "New York"}
)
"""

#delete_dynamodb_table("customer_activity")

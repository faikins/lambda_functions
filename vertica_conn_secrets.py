import os
import json
import vertica_python
import ssl
import boto3
from botocore.exceptions import ClientError

def get_ssl_context():
    """
    Returns an SSL context for Vertica connections if SSL is required.
    Modify as needed.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

def get_secret(secret_name, region_name):
    """
    Retrieves a secret from AWS Secrets Manager and returns it as a dictionary.
    Assumes the secret is stored in JSON format.
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        response = client.get_secret_value(SecretId=secret_name)
    except ClientError as e:
        print(f"Error retrieving secret {secret_name}: {e}")
        raise e

    if 'SecretString' in response:
        secret = response['SecretString']
        return json.loads(secret)
    else:
        # If the secret is stored in binary form.
        secret = response['SecretBinary']
        return json.loads(secret)

def lambda_handler(event, context):
    # Retrieve configuration from environment variables.
    try:
        VERTICA_HOST       = os.environ['VERTICA_HOST']
        VERTICA_PORT       = int(os.environ['VERTICA_PORT'])
        VERTICA_USER       = os.environ['VERTICA_USER']
        VERTICA_DATABASE   = os.environ['VERTICA_DATABASE']
        VERTICA_SECRET_NAME = os.environ['VERTICA_SECRET_NAME']  # Name or ARN of the secret
        USE_SSL            = os.environ.get('USE_SSL', 'False').lower() == 'true'
        AWS_REGION         = os.environ.get('AWS_REGION', 'us-west-2')
    except KeyError as e:
        error_message = f"Missing required environment variable: {e}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": error_message
            })
        }

    # Retrieve the Vertica password from Secrets Manager.
    try:
        secret_data = get_secret(VERTICA_SECRET_NAME, AWS_REGION)
        # Assuming the secret JSON contains a key called "password"
        VERTICA_PASSWORD = secret_data['password']
    except Exception as e:
        error_message = f"Error retrieving secret from Secrets Manager: {e}"
        print(error_message)
        return {
            "statusCode": 500,
            "body": json.dumps({
                "status": "error",
                "message": error_message
            })
        }

    # Configure the Vertica connection.
    connection_info = {
        'host': VERTICA_HOST,
        'port': VERTICA_PORT,
        'user': VERTICA_USER,
        'password': VERTICA_PASSWORD,
        'database': VERTICA_DATABASE,
        'connection_timeout': 10,
        'tlsmode': 'require',
        'ssl': get_ssl_context() if USE_SSL else False
    }

    try:
        print("Connecting to Vertica...")
        conn = vertica_python.connect(**connection_info)
        cursor = conn.cursor()
        print("Connection established. Executing test query...")

        # Execute a simple test query.
        cursor.execute("SELECT 1;")
        result = cursor.fetchone()
        message = f"Connected to Vertica successfully. Query result: {result}"
        status_code = 200
        status = "success"

    except Exception as e:
        message = f"Error connecting to Vertica: {e}"
        status_code = 500
        status = "error"

    finally:
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    return {
        "statusCode": status_code,
        "body": json.dumps({
            "status": status,
            "message": message
        })
    }

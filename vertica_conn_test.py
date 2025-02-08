import os
import json
import vertica_python
import ssl

def get_ssl_context():
    """
    Returns an SSL context for Vertica connections if SSL is required.
    Modify the context settings as needed.
    """
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    context.check_hostname = False
    context.verify_mode = ssl.CERT_NONE
    return context

def lambda_handler(event, context):
    # Read Vertica connection settings from environment variables.
    # Using direct access so that missing variables raise an error.
    try:
        VERTICA_HOST     = os.environ['VERTICA_HOST']
        VERTICA_PORT     = int(os.environ['VERTICA_PORT'])
        VERTICA_USER     = os.environ['VERTICA_USER']
        VERTICA_PASSWORD = os.environ['VERTICA_PASSWORD']
        VERTICA_DATABASE = os.environ['VERTICA_DATABASE']
        # If SSL is required, set USE_SSL to "True" in your Lambda environment.
        USE_SSL          = os.environ.get('USE_SSL', 'False').lower() == 'true'
    except KeyError as e:
        # Return an error if any required environment variable is missing.
        error_message = f"Missing required environment variable: {e}"
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
        # Clean up the connection and cursor.
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass

    # Return a JSON response.
    return {
        "statusCode": status_code,
        "body": json.dumps({
            "status": status,
            "message": message
        })
    }

import datetime
import time
import requests as rq
import json
import boto3

def generate_params(api_config, function, symbol=None):
    """
    Generate API parameters dynamically based on the function name.
    
    :param api_name: The name of the API function (e.g., "OVERVIEW").
    :param symbol: The stock symbol, if required for the API function.
    :return: A dictionary of parameters for the API call.
    """
    # Copy the template parameters for the given API
    params = api_config.get(function, {}).copy()
    
    # Add the symbol if it's required and provided
    if "symbol" in params and symbol:
        params["symbol"] = symbol
    
    return params

def fetch_data(base_url, params, api_call_count):
    """
    Fetch data from url based on params
    """
    if api_call_count % 5 == 0:
        time.sleep(60)
    response = rq.get(base_url, params=params)
    api_call_count += 1
    if response.status_code == 200:
        return response.json(), api_call_count
    else:
        print(f"Error: {response.status_code} - {response.text}")
        return None, api_call_count

def write_json_to_s3(bucket_name, file_prefix, file_name, json_data):
    """
    Writes JSON data to a specified S3 bucket.
    
    :param bucket_name: Name of the S3 bucket
    :param file_name: Name of the file to save in the bucket (e.g., 'data.json')
    :param json_data: The JSON data to write (as a dictionary)
    """
    # Create an S3 client
    s3_client = boto3.client('s3')
    
    try:
        object_key = f"{file_prefix}{file_name}"
        # Convert the dictionary to a JSON string
        json_string = json.dumps(json_data)
        
        # Upload the JSON string to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_string,
            ContentType="application/json"
        )
        print(f"Successfully wrote {file_name} to {bucket_name}")
    except Exception as e:
        print(f"Error writing to S3: {e}")

def lambda_handler(event, context):
    # Specify the file path
    file_path = "credit_rating_project\extract_lambdas\extract_config.json"
    
    # Open the file and load the config data
    with open(file_path, "r") as file:
        api_config = json.load(file)
    
    base_url = api_config.get("url")
    bucket_name = api_config.get("bucket_name")
    
    # Determine schedule type from EventBridge input
    schedule = event.get('schedule', 'weekly')
    timestamp = datetime.datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    if schedule.lower() not in api_config["functions"].keys():
        raise ValueError(f"Invalid schedule type {schedule}")
    
    params_list = []
    api_call_count = 0
    for function in api_config["functions"][schedule]:
        if "symbol" in function.keys():
            for company, symbol in api_config["sector_wise_symbols"].items():
                params = generate_params(api_config, function, symbol)
                params["api_key"] = api_config["api_key"]
                params_list.append(params)
        else:
            params = generate_params(api_config, function, symbol)
            params_list.append(params)
            
    for params in params_list:
        json_data, api_call_count = fetch_data(base_url, params, api_call_count)
        if "symbol" in params.keys():
            file_name = params["function"] + params["symbol"] + "_" + timestamp + "_" + ".json"
            file_path = "financial_data" + + "/" + params["symbol"] + "/"
        else:
            file_name = params["function"] + "_" + timestamp + "_" + ".json"
            file_path = "financial_data" + + "/" + "economic_indicators" + "/"
        
        write_json_to_s3(bucket_name, file_path, file_name, json_data)
# Influxdb  Delete points for a specific field in a specified time range
import certifi
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import InfluxQL_Cloud_write_string
import requests
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()
# Load credential from .env file

token = os.getenv("INFLUX_TOKEN")
org = os.getenv("INFLUX_ORG")
bucket = os.getenv("INFLUX_BUCKET")
# bucket = "test_bucket"
measurement = "energy"  # power consumption points
field = "consumed_today_cost"
start = "2024-06-09T00:00:01Z"
end = "2024-06-09T08:29:53Z"
n = 0
time_list = []
query = f"""from(bucket:"{bucket}")\n|> range(start: {start}, stop: {end})\n|> filter(fn:(r) => r._measurement == "{measurement}")\n|> filter(fn:(r) => r._field == "{field}")"""

api_uri = "https://eu-central-1-1.aws.cloud2.influxdata.com/api/v2/delete"
headers = {"Authorization": "Token " + token, "Content-Type": "application/json"}
params = {"bucket": bucket}


try:
    result = InfluxQL_Cloud_write_string.query(query)
    for table in result:
        for record in table.records:
            time = record.get_time()
            value = record.get_value()
            #format time into 2023-12-01T22:04:59Z and append time to list time_list
            time = time.strftime("%Y-%m-%dT%H:%M:%SZ")
            time_list.append(time)
            n+=1
            print(f"{n}: {time} | Reading: {value}")
except Exception as e:
    print(e)
    print("No data found")
confirm = input("Delete which points? (1..2..3...4, a=all, n=abort): ")
if confirm == "n":
    print("Aborting")
    exit()
if confirm == "1":
    #adjust start to be 1 min before first item in the time_list  and end to be 1 hour after time_list
    #conver string to datetime object and adjust start to be 1 hour before first item in the time_list  and end to be 1 min after time_list
    start = datetime.strptime(time_list[0], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    end = datetime.strptime(time_list[0], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1)
if confirm == "2":
    #conver string to datetime object and adjust start to be 1 hour before first item in the time_list  and end to be 1 min after time_list
    start = datetime.strptime(time_list[1], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    end = datetime.strptime(time_list[1], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1)
if confirm == "3":
    start = datetime.strptime(time_list[2], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    end = datetime.strptime(time_list[2], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1)
if confirm == "4":
    start = datetime.strptime(time_list[3], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    end = datetime.strptime(time_list[3], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1)
if confirm == "a":
    start = datetime.strptime(time_list[0], "%Y-%m-%dT%H:%M:%SZ") - timedelta(hours=1)
    end = datetime.strptime(time_list[-1], "%Y-%m-%dT%H:%M:%SZ") + timedelta(hours=1)
print(f"Deleting points from {start} to {end}")
params = {"bucket": bucket}    
payload = {
#convert datetime to string with format 2023-12-01T22:04:59Z
    "start": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "stop": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
    "predicate": f'_measurement="{measurement}" AND _field="{field}"',
}
response = requests.post(api_uri, headers=headers, json=payload, params=params)
if response.status_code == '204':
    print('Successfully cleared points')
else:
    print(response.status_code)

import certifi
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
import os
from dotenv import load_dotenv

load_dotenv()
# Load credential from .env file

token = os.getenv("INFLUX_TOKEN")
org = os.getenv("INFLUX_ORG")
bucket = os.getenv("INFLUX_BUCKET")

influxdb_client = InfluxDBClient(
    url="https://eu-central-1-1.aws.cloud2.influxdata.com",
    token=token,
    org=org,
    ssl_ca_cert=certifi.where(),
)

write_api = influxdb_client.write_api(write_options=SYNCHRONOUS)
query_api = influxdb_client.query_api()


def write(data):
    write_api.write(bucket, org, data)


def query(query):
    result = query_api.query(org=org, query=query)
    return result

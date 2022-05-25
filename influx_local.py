from datetime import datetime

from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS

# You can generate an API token from the "API Tokens Tab" in the UI
token = "lfKYykGn6ZCmaGhG-Y3FOkjSJi5wMXRWK7F5vV-YLegKPXu3G9HtPJqYhAI-rpBvCAHEYIJRcEEqwee3OaWGPw=="
org = "esi-sna"
bucket = "ecg"

#with InfluxDBClient(url="http://localhost:8086", token=token, org=org) as client:
client = InfluxDBClient(url="http://localhost:8086", token=token, org=org)
write_api = client.write_api(write_options=SYNCHRONOUS)
point = Point("ecg") \
    .tag("host", "host1") \
    .field("used_percent", 1110.43234543) \
    .time(datetime.utcnow(), WritePrecision.NS)

write_api.write(bucket, org, point)

# #Simple Query-------------------------------------------------------------
# query_api = client.query_api()

# query = 'from(bucket: "ecg")\
#  |> range(start: -10m)\
#  |> filter(fn: (r) => r._measurement == "mem")'
# tables = query_api.query(query, org="esi_sba")


# results=[]
# for table in tables:
#   for record in table.records:
#     results.append((record.get_field(), record.get_value()))

# print(results)

client.close()
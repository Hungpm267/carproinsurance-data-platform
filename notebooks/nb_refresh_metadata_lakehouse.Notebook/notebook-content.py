# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {}
# META }

# CELL ********************

import requests, notebookutils

token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")
headers = {"Authorization": f"Bearer {token}"}

r = requests.get(
    "https://api.fabric.microsoft.com/v1/workspaces/e13dac5b-f5b1-4169-bb58-0f6d0bfea366/lakehouses",
    headers=headers
)

for lh in r.json()["value"]:
    print(lh["displayName"], "→", lh["properties"]["sqlEndpointProperties"]["id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

import requests
import json

# Authenticate with Microsoft Fabric API
token = notebookutils.credentials.getToken("https://api.fabric.microsoft.com")

# Configuration
workspace = "e13dac5b-f5b1-4169-bb58-0f6d0bfea366"
lakehouse_sql_endpoint = "5f22b528-3460-46a3-88ad-eac4f0695132"

# API request headers
shared_headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

# Request body with timeout configuration
json_body = {
    "timeout": {
        "timeUnit": "Minutes",
        "value": 2
    }
}

# Refresh SQL Analytics Endpoint metadata
sync_sql_analytics_endpoint = requests.post(
    f"https://api.fabric.microsoft.com/v1/workspaces/{workspace}/sqlEndpoints/{lakehouse_sql_endpoint}/refreshMetadata",
    headers=shared_headers,
    json=json_body
)

# Display the response
display(sync_sql_analytics_endpoint.json())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

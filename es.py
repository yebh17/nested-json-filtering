import json
import numpy as np
import pandas

with open('file.json') as api_search_query:
  api_search_query = json.load(api_search_query)
# create a new list for the Elasticsearch documents
# nested inside the API response object
  elastic_docs = api_search_query["hits"]["hits"]

# print number of documents returned
print ("documents returned:",  len(api_search_query["hits"]["hits"]))

"""
STORE THE ELASTICSEARCH INDEX'S FIELDS IN A DICT
"""
# create an empty dictionary for Elasticsearch fields
fields = {}
# iterate over the document list returned by API call
for num, doc in enumerate(elastic_docs):
# # iterate key-value pairs of the fields dict            
    for key, val in fields.items():
        try:
            fields[key] = np.append(fields[key], val)
        except KeyError:
            fields[key] = np.array([val])
for key, val in fields.items():
    print (key, "--->", val)
    print ("NumPy array len:", len(val), "\n")
#     print (key, "--->", val)
#     print ("NumPy array len:", len(val), "\n")


# """
# CREATE A DATAFRAME OBJECT FROM ELASTICSEARCH
# FIELDS DATA
# """
# # create a Pandas DataFrame array from the fields dict
elastic_df = pandas.DataFrame(fields)
print ('elastic_df:', type(elastic_df), "\n")
print (elastic_df)

# # create a JSON string from the Pandas object
json_data = elastic_df.to_json()
print ("\nto_json() method:", json_data)

# # verify that the to_json() method made a JSON string
try:
    json.loads(json_data)
    print ("\njson_data is a valid JSON string")
except json.decoder.JSONDecodeError as err:
    print ("\njson.decoder.JSONDecodeError:", err)
    print ("json_data is NOT a valid JSON string")


# """
# CREATE SERIES OBJECTS FROM ELASTICSEARCH
# DOCUMENTS
# """
# # create an empty dict for series arrays
elastic_series = {}

# # iterate the docs returned by API call
for num, doc in enumerate(elastic_docs):

#     # get the _id for the doc
    _id = doc["_id"]

    # get source data from document
    source_data = doc['_source']

    # make a Pandas Series object for the doc using _id as key
    elastic_series[_id] = pandas.Series()

    # iterate source data (use iteritems() for Python 2)
    for field, value in source_data.items():

        # set the field type as Series index and value as Series val
        elastic_series[_id].at[field] = value

for key, doc in elastic_series.items():
    print ("\nID:", key, "\n", doc)
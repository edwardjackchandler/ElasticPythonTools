from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv

class ApElastic:

    def __init__(self, port, host):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

    def csv_to_elastic(self, csv_location, index_name, type, index_mappings=None):
        '''
        Take a CSV and ingest it into an Elastic Index.

        :param csv_location:
        :param index_name:
        :param mappingsJSON:
        :return:
        '''

        #if not self.es.indices.exists(index=index_name):

        if not index_mappings == None:
            #CREATE MAPPINGS FROM CSV HERE
            #index_mappings = 'NEW MAPPINGS JSON
            self.create_index(index_name=index_name, index_mappings=index_mappings)

        with open(csv_location) as file:
            reader = csv.DictReader(file)
            helpers.bulk(self.es, reader, index=index_name, doc_type=type)


    def reindex(self, original_index_name, target_index_name, delete_original_index=False, delete_target_index=False):

        if delete_target_index is True:
            self.es.indices.delete(index=target_index_name)

        original_index_mapping = self.es.indices.get_mapping(original_index_name)[original_index_name]


        print(original_index_mapping)
        #"{'mappings': {'character': {'properties': {'gender': {'type': 'keyword'}, 'id': {'type': 'keyword'}, 'name': {'type': 'text'}, 'normalized_name': {'type': 'text'}}}}}"

        self.create_index(index_name=target_index_name, index_mappings=original_index_mapping)

        reindex_body = {
            "source": {
                "index": original_index_name,
                "query": {
                    "match_all": {}
                }
            },
            "dest": {
                "index": target_index_name
            }
        }

        print("reindex_body")
        print(reindex_body)

        self.es.reindex(body=reindex_body)
        print("reindex")

        if delete_original_index is True:
            self.es.indices.delete(index=original_index_name)


    def create_index(self, index_name, index_mappings):
        '''

        :param index_mappings:
        :return:
        '''

        self.es.indices.create(index=index_name, body=index_mappings)


    '''
    Add an additional field to an index
    '''
    def add_property_mapping(self, index_name, type, property_name, property_type):

        mapping = {
            "properties": {
                property_name: {
                    "type": property_type
                }
            }
        }

        self.es.indices.put_mapping(doc_type=type, body=mapping, index=index_name)


    def add_mapping_parameter(self, index_name, type, property_name, paramter_name, parameter_value):
        mapping = {
            "properties": {
                property_name: {
                    paramter_name: parameter_value
                }
            }
        }

        self.es.indices.put_mapping(doc_type=type, body=mapping, index=index_name)


    def add_property_value(self, index_name, type, property_name, property_value, doc_id):
        self.es.update(index=index_name, doc_type=type, body=self.__add_field_json(property_name, property_value), id=doc_id)


    def add_property_all_docs(self, index_name, type, property_name, property_value, search_query="*"):

        docs = self.es.search(index=index_name, size=10000, _source=False, q=search_query + " AND !{0}:{1}".format(property_name, property_value))

        for doc in docs['hits']['hits']:
            print("Update ID: " + doc['_id'])
            self.es.update(index=index_name, doc_type=type, body=self.__add_field_json(property_name, property_value), id=doc['_id'])

    # JSON for adding field
    def __add_field_json(self, property_name, property_value):
        doc_update = {
            "doc": {
                    property_name: property_value
            }

        }

        return doc_update
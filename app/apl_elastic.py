from elasticsearch import Elasticsearch
from elasticsearch import helpers
import csv

class AplElastic:

    def __init__(self, port, host):
        self.es = Elasticsearch([{'host': 'localhost', 'port': 9200}])



    def csv_to_elastic(self, csv_location, index_name, index_mappings=None):
        '''
        Take a CSV and ingest it into an Elastic Index.

        :param csv_location:
        :param index_name:
        :param mappingsJSON:
        :return:
        '''



        if not self.es.indices.exists(index=index_name):
            if index_mappings == None:
                #CREATE MAPPINGS FROM CSV HERE
                index_mappings = 'NEW MAPPINGS JSON'

            self.create_index(index_mappings)

        with open(csv_location) as file:
            reader = csv.DictReader(file)
            helpers.bulk(self.es, reader, index=index_name, doc_type='my-type')



    def __csv_ingest(self, csv_location, index_name):
        '''
        Ingest a CSV into Elasticsearch

        :param csv_location:
        :param index_name:
        :return:
        '''
        csv = pd.DataFrame.from_csv(csv_location)

    #WORKING BUT NEED TO HANDLE THE TIMEOUT ERRORS
    def reindex(self, original_index_name, target_index_name, delete_original_index=False, delete_target_index=False):

        if delete_target_index is True:
            self.es.indices.delete(index=target_index_name)

        original_index_mapping = self.es.indices.get_mapping(original_index_name)[original_index_name]

        #"{'mappings': {'character': {'properties': {'gender': {'type': 'keyword'}, 'id': {'type': 'keyword'}, 'name': {'type': 'text'}, 'normalized_name': {'type': 'text'}}}}}"

        original_index_mapping = {'mappings':
                                      {'character':
                                           {'properties':
                                                {'gender': {'type': 'keyword'},
                                                'id': {'type': 'keyword'},
                                                'name': {'type': 'text'},
                                                'normalized_name': {'type': 'text'}}}}}


        self.create_index(index_name=target_index_name, index_mappings=original_index_mapping)


        reindex_body = {
            "source": {
                "index": original_index_name,
                "type": "script",
                "query": {
                    "match_all": {}
                }
            },
            "dest": {
                "index": target_index_name
            }
        }

        self.es.reindex(body=reindex_body)

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
    def index_add_field(self):
        print("Not Yet Implemented")
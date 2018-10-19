import app.apl_elastic as apl_elastic

csv_location = "C:\\Users\\jchandler\\Documents\\Datasets\\Simpsons\\simpsons_characters.csv"

apl = apl_elastic.AplElastic(host='localhost', port='9200')

characters_template = {
            "mappings": {
                "character": {
                    "properties": {
                        "id": {
                            "type": "keyword"
                        },
                        "name": {
                            "type": "text"
                        },
                        "normalized_name": {
                            "type": "text"
                        },
                        "gender": {
                            "type": "keyword"
                        }
                    }
                }
            }
        }

#apl.create_index(index_name="idx_simpsons_characters", index_mappings=characters_template)


# apl.csv_to_elastic(csv_location=csv_location,
#                    index_name="idx_simpsons_characters",
#                    type="character",
#                    index_mappings=characters_template
#                    )


#apl.reindex(original_index_name="idx_simpsons_characters_with_mappings", target_index_name="idx_simpsons_characters_copy2", delete_target_index=True)

#apl.reindex(original_index_name="idx_simpsons_characters", target_index_name="idx_simpsons_characters_copy2", delete_target_index=True)

#apl.add_property_mapping(index_name="idx_simpsons_characters_copy2", type="character", property_name="race", property_type="keyword")

#apl.add_property_value(index_name="idx_simpsons_characters_copy2", type="character", property_name="race", property_value="maggie", doc_id="AV6Q17Jq8ONIOQTCvcQs")

apl.add_property_all_docs(index_name="idx_simpsons_characters_copy2", type="character", property_name="race", property_value="maggie")


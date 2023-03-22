import singer

class Stream:
    def __init__(self, config, state, catalog):
        self.config = config
        self.catalog = catalog
        self.state = state

class DogBreeds(Stream):
    stream_id = "dog_breeds"
    stream_name = 'dog_breeds'
    key_properties = ['id']
    replication_method = 'FULL_TABLE'
    replication_keys = []



STREAM_OBJECTS = {
    'dog_breeds': DogBreeds,
}
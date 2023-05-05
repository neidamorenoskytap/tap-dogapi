#!/usr/bin/env python3
import os
import json
import requests
import singer
from singer import utils, metadata, Transformer
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema
from .streams import STREAM_OBJECTS

api_endpoint = "https://api.thedogapi.com/v1/breeds"
REQUIRED_CONFIG_KEYS = ["api_key"]
# what does this logger thing do?
LOGGER = singer.get_logger()

# I added this
def make_api_request(url, api_key):
    response = requests.get(url, headers={"x-api-key":api_key})
    return response.json()

def get_abs_path(path):
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    """ Load schemas from schemas folder """
    schemas = {}
    for filename in os.listdir(get_abs_path('schemas')):
        path = get_abs_path('schemas') + '/' + filename
        file_raw = filename.replace('.json', '')
        with open(path) as file:
            schemas[file_raw] = Schema.from_dict(json.load(file))
    return schemas


def discover():
    raw_schemas = load_schemas()
    streams = []
    for stream_id, schema in raw_schemas.items():
        # TODO: populate any metadata and stream's key properties here..\

        key_properties = ['id']
        stream_metadata = metadata.get_standard_metadata(
            schema=schema.to_dict(),
            key_properties=key_properties
        )
        
        streams.append(
            CatalogEntry(
                tap_stream_id=stream_id,
                stream=stream_id,
                schema=schema,
                key_properties=key_properties,
                metadata=stream_metadata,
                replication_key=None,
                is_view=None,
                database=None,
                table=None,
                row_count=None,
                stream_alias=None,
                replication_method=None,
            )
        )
    return Catalog(streams)


def sync(config, state, catalog):
    """ Sync data from tap source """
    # Loop over selected streams in catalog
    for stream in catalog.get_selected_streams(state):
        stream_id = stream.tap_stream_id
        stream_schema = stream.schema
        stream_object = STREAM_OBJECTS.get(stream_id)(config, state, catalog)

        if stream_object is None:
            raise Exception("This stream is unknown".format(stream_id))
        

        # bookmark_column = stream.metadata[0]["metadata"]["replication-key"]
        # print("bookmark column is ", bookmark_column)
        # is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value

        singer.write_schema(
            stream_name=stream_id,
            schema=stream_schema.to_dict(),
            key_properties=stream_object.key_properties,
        )

        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        # Getting API data
        tap_data = make_api_request(api_endpoint, config.get("api_key"))

        max_bookmark = None
        with Transformer() as transformer:
            for row in tap_data:
            # write one or more rows to the stream:
                singer.write_record(stream_id, 
                                    transformer.transform(row,
                                                          stream_schema.to_dict(),
                                                          metadata.to_map(stream.metadata)
                                                          )
                                    )
            #     if bookmark_column:
            #         if is_sorted:
            #         # update bookmark to latest value
            #             singer.write_state({stream.tap_stream_id: {bookmark_column: row[bookmark_column]}})
            #         else:
            #         # if data unsorted, save max value until end of writes
            #             max_bookmark = max(max_bookmark, row[bookmark_column])
            # if bookmark_column and not is_sorted:
            #     singer.write_state({stream.tap_stream_id: {bookmark_column: max_bookmark}})
    return


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = discover()
        catalog.dump()
    # Otherwise run in sync mode
    else:
        if args.catalog:
            catalog = args.catalog
        else:
            catalog = discover()
        sync(args.config, args.state, catalog)


if __name__ == "__main__":
    main()

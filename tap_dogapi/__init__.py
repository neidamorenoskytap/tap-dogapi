#!/usr/bin/env python3
import os
import json
import requests
import singer
from singer import utils, metadata
from singer.catalog import Catalog, CatalogEntry
from singer.schema import Schema

api_endpoint = "https://api.thedogapi.com/v1/breeds"
REQUIRED_CONFIG_KEYS = ["api_key"]
# what does this logger thing do?
LOGGER = singer.get_logger()

# I added this
def make_api_request(url, headers):
    response = requests.get("url", headers={headers})
    return response

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
        # I added this
        stream_metadata = [{
  "metadata" : {
    "selected" : True,
    "table-key-properties": ["id"],
    "schema-name": "dog_breeds",
  },
  "breadcrumb": []
}]
        key_properties = ['id']
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
        LOGGER.info("Syncing stream:" + stream.tap_stream_id)

        bookmark_column = stream.replication_key
        is_sorted = True  # TODO: indicate whether data is sorted ascending on bookmark value
        # not sure what this todo is asking ^

        singer.write_schema(
            stream_name=stream.tap_stream_id,
            schema=stream.schema,
            key_properties=stream.key_properties,
        )

        # TODO: delete and replace this inline function with your own data retrieval process:
        # tap_data = lambda: [{"id": x, "name": "row${x}"} for x in range(1000)]
        # here is where I get the data from API
        # I added this
        tap_data = make_api_request(api_endpoint, config.get("api_key"))

        max_bookmark = None
        for row in tap_data():
            # TODO: place type conversions or transformations here
            # again, not sure what this todo is saying ^

            # write one or more rows to the stream:
            singer.write_records(stream.tap_stream_id, [row])
            if bookmark_column:
                if is_sorted:
                    # update bookmark to latest value
                    singer.write_state({stream.tap_stream_id: row[bookmark_column]})
                else:
                    # if data unsorted, save max value until end of writes
                    max_bookmark = max(max_bookmark, row[bookmark_column])
        if bookmark_column and not is_sorted:
            singer.write_state({stream.tap_stream_id: max_bookmark})
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

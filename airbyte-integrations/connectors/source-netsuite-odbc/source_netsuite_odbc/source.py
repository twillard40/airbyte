#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import logging
from datetime import datetime
from typing import Mapping, Tuple, Any, List, Optional, Iterable

from airbyte_cdk.sources.streams import Stream
from airbyte_cdk.sources import AbstractSource
from source_netsuite_odbc.discover_utils import NetsuiteODBCTableDiscoverer
from .streams import NetsuiteODBCStream
from .odbc_utils import NetsuiteODBCCursorConstructor



class SourceNetsuiteOdbc(AbstractSource):
    logger: logging.Logger = logging.getLogger("airbyte")
    
    def streams(self, config: Mapping[str, Any]) -> Iterable[Stream]:
        cursor_constructor = NetsuiteODBCCursorConstructor()
        db_connection = cursor_constructor.create_database_connection(config)
        discoverer = NetsuiteODBCTableDiscoverer(db_connection)
        streams = discoverer.get_streams()
        number_streams = 0
        for stream in streams:
            stream_name = stream.name
            number_streams = number_streams + 1
            netsuite_stream = NetsuiteODBCStream(db_connection=db_connection, table_name=stream_name, stream=stream, config=config)
            yield netsuite_stream
        self.logger.info(f"Finished generating streams.  Discovered {number_streams} streams.")
    
    def check_connection(self, logger: logging.Logger, config: Mapping[str, Any]) -> Tuple[bool, Optional[Any]]:
        """
        :param logger: source logger
        :param config: The user-provided configuration as specified by the source's spec.
          This usually contains information required to check connection e.g. tokens, secrets and keys etc.
        :return: A tuple of (boolean, error). If boolean is true, then the connection check is successful
          and we can connect to the underlying data source using the provided configuration.
          Otherwise, the input config cannot be used to connect to the underlying data source,
          and the "error" object should describe what went wrong.
          The error object will be cast to string to display the problem to the user.
        """
        try:
            cursor_constructor = NetsuiteODBCCursorConstructor()
            db_connection = cursor_constructor.create_database_connection(config)

            db_connection.execute("SELECT * FROM OA_TABLES")
            db_connection.fetchone()
            return True, None
        except Exception as e:
            logger.error(e)
            return False, e

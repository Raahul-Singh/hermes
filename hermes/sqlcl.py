#!/usr/bin/python3
"""
Inspired by the SQLCL tool by Tamas Budavari
Modified by Raahul Singh to work with Python 3
"""

import argparse
import io
import logging
import os
import sys
from urllib.parse import urlencode

import pandas as pd
import requests

SUPPORTED_OUTPUT_FORMATS = ["html", "csv", "json"]


class SQLCL:
    # TODO: Add support for running multiple queries in a single request

    def __init__(
        self,
        url="https://skyserver.sdss.org/dr16/en/tools/search/x_sql.aspx",  # Using DR16. Breaks on DR18 due to changes in the ASP interface. See if fix needed.
        output_format="csv",
        log_to_stdout=True,
    ):
        self.url = url
        if output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"Invalid output format {output_format}. Supported formats are {SUPPORTED_OUTPUT_FORMAT}"
            )
        self.output_format = output_format

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if log_to_stdout:
            self.logger.addHandler(logging.StreamHandler(sys.stdout))

    def filtercomment(self, sql):
        """Get rid of comments starting with --"""
        fsql = ""
        for line in sql.split("\n"):
            fsql += line.split("--")[0] + " " + os.linesep
        return fsql

    def query_database(self, query):
        # URL-encode the query
        query = self.filtercomment(query)
        self.logger.info(f"Querying the database with the following query: {query}")
        encoded_query = urlencode({"cmd": query, "format": self.output_format})

        # Construct the full URL with the encoded query
        full_url = f"{self.url}?{encoded_query}"

        # Send a GET request to the provided URL
        response = requests.get(full_url)

        # Check if the request was successful
        if response.status_code == 200:
            self.logger.info("Query successful")
            # TODO: Attempt to parse the response as JSON
            # Attempt to parse the response as CSV
            if self.output_format == "csv":
                urlData = response.content
                return pd.read_csv(io.StringIO(urlData.decode("utf-8")), skiprows=1)
            else:
                return {
                    "error": "Failed to parse the response. Unsupported output format: {}".format(
                        self.output_format
                    )
                }
        else:
            return {
                "error": "Failed to query the database. Status code: {}".format(
                    response.status_code
                )
            }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="SQLCL command line query tool")
    parser.add_argument(
        "--url", type=str, help=f"URL with the ASP interface (default: {self.url})"
    )
    parser.add_argument(
        "--format",
        type=str,
        choices=SUPPORTED_OUTPUT_FORMATS,
        help=f"set output format such as {SUPPORTED_OUTPUT_FORMATS} (default: {self.output_format})",
    )
    parser.add_argument(
        "--query", type=str, action="append", help="specify query on the command line"
    )

    args = parser.parse_args()
    sqlcl = SQLCL(url=args.url, output_format=args.format)

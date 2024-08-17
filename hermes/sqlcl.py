#!/usr/bin/python3
"""
Inspired by the SQLCL tool by Tamas Budavari
Modified by Raahul Singh to work with Python 3
"""

import argparse
import io
import os
from io import StringIO
from urllib.parse import urlencode

import pandas as pd
import requests

SUPPORTED_OUTPUT_FORMATS = ["html", "csv", "json"]


class SQLCL:
    # TODO: Add support for running multiple queries in a single request

    def __init__(
        self,
        url="https://skyserver.sdss.org/dr12/en/tools/search/x_sql.aspx",
        output_format="csv",
    ):
        self.url = url
        if output_format not in SUPPORTED_OUTPUT_FORMATS:
            raise ValueError(
                f"Invalid output format {output_format}. Supported formats are {SUPPORTED_OUTPUT_FORMAT}"
            )
        self.output_format = output_format

    def filtercomment(self, sql):
        """Get rid of comments starting with --"""
        fsql = ""
        for line in sql.split("\n"):
            fsql += line.split("--")[0] + " " + os.linesep
        return fsql

    def query_database(self, query):
        # URL-encode the query
        query = self.filtercomment(query)
        encoded_query = urlencode({"cmd": query, "format": self.output_format})

        # Construct the full URL with the encoded query
        full_url = f"{self.url}?{encoded_query}"

        # Send a GET request to the provided URL
        response = requests.get(full_url)

        # Check if the request was successful
        if response.status_code == 200:
            # Attempt to parse the response as JSON
            if self.output_format == "json":
                return response.json()
            # Attempt to parse the response as CSV
            elif self.output_format == "csv":
                urlData = response.content
                return pd.read_csv(io.StringIO(urlData.decode("utf-8")), skiprows=1)
                # return pd.read_csv(StringIO(response.text))
            # Attempt to parse the response as HTML
            elif self.output_format == "html":
                return pd.read_html(StringIO(response.text))
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

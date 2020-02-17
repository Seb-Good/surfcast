"""
noaa_map_file.py
----------------
This module provide a class and methods for processing map files.
By: Sebastian D. Goodfellow, Ph.D., 2018
"""

# 3rd party imports
import time
import requests
import numpy as np
import pandas as pd


# Local imports
from surfcast import MAP_URL, MAP_ATTRIBUTES


class NOAAMapFile(object):

    def __init__(self, filename):

        # Set parameters
        self.filename = filename

        # Set attributes
        self.text_file = self._download_file()
        self.grid_count = len(self.text_file)
        self.map_data = self._get_map_data()

    def _download_file(self):
        """This function will download from the NOAA map text file corresponding to the filename
        input by the user and return a row delimited text file."""
        """Get HTML object from url"""
        text_file = None
        print('Downloading NOAA map file {}'.format(self.filename))
        while text_file is None:
            try:
                # Send file request to server and download
                start_time = time.time()
                response = requests.get(url=MAP_URL + self.filename, verify=False, timeout=60)
                print('Download complete: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

                # Parse text file by line breaks
                start_time = time.time()
                text_file = [row for row in response.text.split('\n')]
                print('Text parsing complete: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

                return text_file

            except Exception:
                print('Connection Error, retrying...')
                time.sleep(1)
                pass

    def _get_map_data(self):
        """Extract grid data from map file and format as DataFrame."""
        # List to collect grid data
        grid_data = list()

        # Set start time
        start_time = time.time()

        for row in self.text_file:
            if len(row.split()) != 0:

                # Parse row string
                row = row.split()

                # Create row dictionary
                row_dict = {key['name']: key['dtype'](val) for key, val in zip(MAP_ATTRIBUTES, row)}

                # Collect grid data
                grid_data.append(row_dict)

        print('Map data formatted: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

        return pd.DataFrame(grid_data)

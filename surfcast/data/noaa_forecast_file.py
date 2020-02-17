"""
noaa_forecast_file.py
---------------------
This module provide a class and methods for processing NCAST and FCAST files.
By: Sebastian D. Goodfellow, Ph.D., 2018
"""

# 3rd party imports
import time
import requests
import numpy as np
import pandas as pd
import timeout_decorator
from datetime import datetime


# Local imports
from surfcast import FILE_ATTRIBUTES


class NOAAForecastFile(object):

    def __init__(self, url, filename, filetype, lake, verbose=False):

        # Set parameters
        self.url = url
        self.filename = filename
        self.filetype = filetype
        self.lake = lake
        self.verbose = verbose

        # Set attributes
        start_time = time.time()
        self.text_file = self._download_file()
        self.grid_count = self._get_grid_count()
        self.hour_count = self._get_hour_count()
        self.row_count = int(self.hour_count * self.grid_count)
        self.map_name = self._get_map_name()
        self.grid_data = self._get_grid_data()
        print('{} {} processed: {} minutes'.format(self.lake, self.filename,
                                                   np.round((time.time() - start_time) / 60., 4)))

    def _download_file(self):
        """This function will download from the NOAA database text file corresponding to the filename and url
        input by the user and return a row delimited text file."""
        """Get HTML object from url"""
        text_file = None
        if self.verbose:
            print('Downloading NOAA file {}'.format(self.filename))
        while text_file is None:
            try:
                # Send file request to server and download
                start_time = time.time()
                response = requests.get(self.url + self.filename, verify=False, timeout=120)
                if self.verbose:
                    print('Download complete: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

                # Parse text file by line breaks
                start_time = time.time()
                text_file = [row for row in response.text.split('\n')]
                if self.verbose:
                    print('Text parsing complete: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

                return text_file

            except Exception:
                if self.verbose:
                    print('Connection Error, retrying...')
                time.sleep(1)
                pass

    def _get_grid_count(self):
        """Get number of grid point in file."""
        return int(self.text_file[0].split()[-1])

    def _get_map_name(self):
        """Get the grid map name for file."""
        map_name = self.text_file[0].split()[3].split('/')[-1]
        map_name = '{}.{}'.format(map_name.split('.')[0], 'map')
        if self.lake == 'superior':
            map_name = 'superior' + map_name.split('sup')[1]

        return map_name

    def _get_hour_count(self):
        """Get number of hours in file."""
        return len([row for row in self.text_file if 'dat' in row])

    def _get_grid_data(self):
        """Extract grid data from text file and save to DataFrame."""
        # Set start time
        start_time = time.time()

        # Set current datetime
        datetime_current = None

        # List to collect grid data
        grid_data = list()

        # Loop through test file rows
        if self.verbose:
            print('Processing grid data: {} hours, '
                  '{} grid points, {} rows, {} attributes'.format(self.hour_count, self.grid_count, self.row_count,
                                                                  len(FILE_ATTRIBUTES[self.filetype])))
        for row in self.text_file:

            # Check for header
            if 'dat' in row:

                # Parse row string
                row = row.split()

                # Set datetime from header
                datetime_current = datetime.strptime(row[0] + row[1] + row[2], "%Y%j%H")

            elif 'dat' not in row and len(row.split()) != 0:

                # Parse row string
                row = row.split()

                # Create row dictionary
                row_dict = {'datetime': datetime_current, 'grid_number': np.int16(row[0])}
                row_dict.update({key: val for key, val in zip(FILE_ATTRIBUTES[self.filetype], row[1:])})

                # Collect grid data
                grid_data.append(row_dict)

            else:
                pass

        if self.verbose:
            print('Grid data formatting complete: {} minutes\n'.format(np.round((time.time() - start_time) / 60., 4)))

        return pd.DataFrame(grid_data)

"""
noaa_db.py
----------
This module provide a class and methods for extracting the most recent NCAST and FCAST files.
By: Sebastian D. Goodfellow, Ph.D., 2018
"""

# 3rd party imports
import os
import time
import requests
import pandas as pd
from dateutil import tz
from bs4 import BeautifulSoup
from datetime import datetime

# Local imports
from surfcast import DATA_DIR, NOAA_URL, EXTENSIONS, LAKES


class NOAADB(object):

    """
    Class converts the NCAST and FCAST FTP database list into a Pandas DataFrame.

    The gridded fields filename format is: LYYYYDDDHH.N.EXT

    L    - lake letter (s=Superior, m=Michigan, h=Huron, e=Erie, o=Ontario)
    YYYY - year at start of simulation (GMT)
    DDD  - Day Of Year at start of simulation (GMT)
    HH   - hr at start of simulation (GMT)
    N    - Site Number
    """

    def __init__(self, process=True):

        # Set parameters
        self.process = process

        # Set attributes
        self.ncast_db = None
        self.fcast_db = None
        self.current_datetime_GMT = datetime.utcnow().replace(tzinfo=tz.gettz('GMT'))

        # Update database
        if self.process:
            self.generate(db_type='NCAST')
            self.generate(db_type='FCAST')

    def generate(self, db_type):
        """Generate current NCAST or FCAST database."""
        print('Pulling {} files...'.format(db_type.upper()))
        # Get HTML from database page
        html_obj = self._get_html_object(db_type=db_type.upper())

        # Get filename dictionaries from url
        filename_dicts = self._get_filename_dicts(html_obj=html_obj, db_type=db_type.upper())

        # Get DataFrame attribute
        setattr(self, '{}_db'.format(db_type.lower()), pd.DataFrame(filename_dicts))
        getattr(self, '{}_db'.format(db_type.lower())).sort_values(by=['file_datetime', 'lake', 'extension'],
                                                                   inplace=True, ascending=False)
        getattr(self, '{}_db'.format(db_type.lower())).reset_index(drop=True, inplace=True)

        # Save DataFrame as CSV
        getattr(self, '{}_db'.format(db_type.lower())).to_csv(
            os.path.join(DATA_DIR, 'noaa_db_{}.csv'.format(db_type.lower())), index=False)
        print('Complete.')

    def _get_filename_dicts(self, html_obj, db_type):
        """Get filename from html object."""
        # List for accepted filenames
        filename_dicts = list()

        # Loop through html tags
        for link in html_obj.findAll('a', href=True):

            # Get filename
            filename = link.contents[0]

            # Check filename
            if self._check_filename(filename=filename):
                filename_dicts.append(self._get_filename_dict(filename=filename, db_type=db_type))
            else:
                pass

        return filename_dicts

    def _get_filename_dict(self, filename, db_type):
        """Generate a filename dictionary from a filename."""
        # File extension
        extension = filename.split('.')[-1]

        # Lake label
        lake = filename[0]

        return {'filename': filename, 'extension': extension, 'filetype': EXTENSIONS[extension],
                'lake': LAKES[lake], 'file_datetime': self._get_file_datetime(filename=filename),
                'current_datetime': self.current_datetime_GMT, 'forecast': db_type,
                'url': '{}{}/'.format(NOAA_URL, db_type)}

    @staticmethod
    def _get_html_object(db_type):
        """Get HTML object from url"""
        html_obj = None
        while html_obj is None:
            try:
                # Get HTML from database page
                html = requests.get('{}{}/'.format(NOAA_URL, db_type.upper()))

                # Create BeautifulSoup object
                html_obj = BeautifulSoup(html.content)

                return html_obj

            except Exception:
                print('Connection Error, retrying...')
                time.sleep(1)
                pass

    @staticmethod
    def _get_file_datetime(filename):
        """Extract the datetime as GMT from file name"""
        file_datetime = datetime.strptime(filename.split('.')[0][1:len(filename.split('.')[0])], "%Y%j%H")
        file_datetime = file_datetime.replace(tzinfo=tz.gettz('GMT'))
        return file_datetime

    @staticmethod
    def _check_filename(filename):
        """Check if filename is of interest."""
        if filename.split('.')[-1] in EXTENSIONS.keys() and filename[0] in LAKES.keys():
            return True
        else:
            return False

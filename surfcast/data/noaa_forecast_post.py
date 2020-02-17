"""
noaa_forecast_post.py
---------------------
This module provide a class and methods for processing NCAST and FCAST forecast posts.
By: Sebastian D. Goodfellow, Ph.D., 2018
"""

# 3rd party imports
import time
import numpy as np
import pandas as pd
from functools import reduce
from joblib import Parallel, delayed

# Local imports
from surfcast import LAKES
from surfcast.data.noaa_forecast_file import NOAAForecastFile


class NOAAForecastPost(object):

    def __init__(self, df, datetime, db_type):

        # Set parameters
        self.df = df
        self.datetime = datetime
        self.db_type = db_type

        # Set attributes
        start_time = time.time()
        print('{} {} forecast processing...'.format(self.db_type, self.datetime))
        self.lake_posts = self._process_post_parallel()
        print('Processing complete: {} minutes'.format(np.round((time.time() - start_time) / 60., 4)))

    def _process_post(self):
        """Process a NOAA forecast post."""
        # Empty list for post
        lake_posts = dict()

        # Loop through post files
        for lake in self.df['lake'].unique():

            # Process lake post
            lake_posts[lake] = NOAALakePost(df=self.df[self.df['lake'] == lake], datetime=self.datetime,
                                            db_type=self.db_type, lake=lake)

        return lake_posts

    def _process_post_parallel(self):
        """Parallel process a NOAA forecast post."""
        # Processes lake posts
        outputs = Parallel(n_jobs=-1)(delayed(self._process_lake_post)(idx, self.df, self.db_type, self.datetime)
                                      for idx in range(len(LAKES)))

        # Gather data
        lake_posts = {output.lake: output for output in outputs}

        return lake_posts

    @staticmethod
    def _process_lake_post(idx, df, db_type, datetime):
        """Wrapper for NOAALakePost for parallel calls."""
        # Get inputs
        lake = list(LAKES.values())[idx]

        return NOAALakePost(df=df[df['lake'] == lake], datetime=datetime, db_type=db_type, lake=lake)


class NOAALakePost(object):

    def __init__(self, df, datetime, db_type, lake):

        # Set parameters
        self.df = df
        self.datetime = datetime
        self.db_type = db_type
        self.lake = lake

        # Set attributes
        self.noaa_files = list()
        self.year = self.datetime.split('-')[0]
        self.filenames = list()
        self.grid_count = None
        self.hour_count = None
        self.row_count = None
        self.map_name = None
        self.grid_data = None

        # Process lake post
        self._process_lake()

    def _process_lake(self):
        """Process specific lake post."""
        # Loop through files
        for idx, df_index, noaa_file in zip(range(self.df.shape[0]), self.df.index, self.df['filename']):

            # Process NOAA file
            noaa_file = NOAAForecastFile(url=self.df.loc[df_index, 'url'],
                                         filename=self.df.loc[df_index, 'filename'],
                                         filetype=self.df.loc[df_index, 'filetype'],
                                         lake=self.df.loc[df_index, 'lake'],
                                         verbose=False)
            self.noaa_files.append(noaa_file)
            self.filenames.append(noaa_file.filename)

            if idx == 0:
                self.grid_count = noaa_file.grid_count
                self.hour_count = noaa_file.hour_count
                self.row_count = noaa_file.row_count
                self.map_name = noaa_file.map_name

        # Concatenate grid data
        self.grid_data = reduce(lambda left, right: pd.merge(left, right, on=['datetime', 'grid_number']),
                                [file.grid_data for file in self.noaa_files])
        self.grid_data['map'] = self.map_name
        self.grid_data['lake'] = self.lake

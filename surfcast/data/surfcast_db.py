"""
surfcast_db.py
--------------
This module provide a class and methods for creating and updating NCAST and FCAST databases.
By: Sebastian D. Goodfellow, Ph.D., 2018
"""

# 3rd party imports
import os
import sqlite3
import pandas as pd
from datetime import datetime

# Local imports
from surfcast.data.noaa_db import NOAADB
from surfcast import DATA_DIR, MAP_FILES, LAKES
from surfcast.data.noaa_map_file import NOAAMapFile
from surfcast.data.noaa_forecast_post import NOAAForecastPost


class SurfcastDB(object):

    def __init__(self):

        # Set attributes
        self.connection = None
        self.cursor = None

        # Create SQLite DB
        self._connect_to_db()

    def update_files_tables(self):
        """Update NCAST and FCAST files database with most recent files in NOAA database."""
        print('Pulling most recent NOAA files...')
        # Get current NOAA database
        noaa_db = NOAADB(process=True)

        # Update NCAST files
        print('\nUpdating NCAST files...')
        self._df_to_table(df=noaa_db.ncast_db, db_type='ncast')

        # Update FCAST files
        print('Updating FCAST files...')
        self._df_to_table(df=noaa_db.fcast_db, db_type='fcast')

    def update_grid_data_tables(self):
        """Update NCAST and FCAST grid data database with most recent files in NOAA database."""
        # Update grid data for all non-committed NCAST NOAA files
        self._update_grid_data_table(db_type='ncast')

        # Update grid data for all non-committed NCAST NOAA files
        self._update_grid_data_table(db_type='fcast')

    def _update_grid_data_table(self, db_type):
        """Update NCAST or FCAST grid data database with most recent files in NOAA database."""
        # Get DataFrame of all non-committed NOAA files
        df = pd.read_sql_query('select * from {}_files where committed is null;'.format(db_type), self.connection)
        print('Processing {} {} forecasts...'.format(len(df['file_datetime'].unique()), db_type.upper()))

        # Loop through unique datetimes
        for idx, date_time in enumerate(df['file_datetime'].unique()):

            # Process forecast post
            forecast_post = NOAAForecastPost(df=df[df['file_datetime'] == date_time],
                                             datetime=date_time, db_type=db_type)

            # Push forecast
            self._push_forecast_post(forecast_post=forecast_post, db_type=db_type)

            print('Processed {} / {} {} forecasts'.format(idx+1, len(df['file_datetime'].unique()), db_type.upper()))

    def _push_forecast_post(self, forecast_post, db_type):
        """Push forecast post into SQLite tables."""
        # Loop through lake posts
        for lake, post in forecast_post.lake_posts.items():

            # Push grid data
            self._push_grid_data(post=post, db_type=db_type)

            # Update files table with grid attributes
            self._update_files_table_grid_attributes(post=post, db_type=db_type)

    def _push_grid_data(self, post, db_type):
        """Push grid data from forecast and lake combination."""
        # Check if table exists
        self._create_grid_data_table(db_type=db_type, year=post.year, lake=post.lake)

        # Push grid data
        post.grid_data.to_sql(name='{}_{}_{}_grid_data'.format(post.lake, post.year, db_type), con=self.connection,
                              if_exists='append', index=False)

    def _update_files_table_grid_attributes(self, post, db_type):
        """Update files table with grid attributes (grid count, hours, rows)."""
        # Loop through lake filenames
        for filename in post.filenames:

            # Update files table with grid attributes
            values = ('true', '{}_{}_{}_grid_data'.format(post.lake, post.year, db_type),
                      post.grid_count, post.hour_count, post.row_count, post.map_name, filename)
            self.cursor.execute('update {}_files set committed=?, table_name=?, grid_count=?, hour_count=?, '
                                'row_count=?, map_name=? where filename=?'.format(db_type), values)
            self.connection.commit()

    def _connect_to_db(self):
        """Connect to SQLite database."""
        if not os.path.isfile(os.path.join(DATA_DIR, 'surfcast_db.sqlite3')):
            print('Creating new database...\n')
            self._create_sqlite_db()
        else:
            print('Connecting to existing database...\n')
            self.connection = sqlite3.connect(os.path.join(DATA_DIR, 'surfcast_db.sqlite3'))
            self.cursor = self.connection.cursor()

    def _create_sqlite_db(self):
        """Create a SQLite database if one does not exist."""
        # Create database connection
        self.connection = sqlite3.connect(os.path.join(DATA_DIR, 'surfcast_db.sqlite3'))

        # Create cursor
        self.cursor = self.connection.cursor()

        # Add NCAST files table
        self._create_files_table(db_type='ncast')

        # Add NCAST grid data table
        self._create_grid_data_tables(db_type='ncast')

        # Add FCAST files table
        self._create_files_table(db_type='fcast')

        # Add FCAST files table
        self._create_grid_data_tables(db_type='fcast')

        # Add map file tables
        self.create_map_file_tables()

        # Add surf spots tables
        self._create_surf_spot_table()

    def _create_files_table(self, db_type):
        """Create NCAST or FCAST files table."""
        self.cursor.execute(
            'create table if not exists {}_files (committed, table_name, filename, extension, '
            'filetype, lake, file_datetime, current_datetime, forecast, url, grid_count, '
            'hour_count, row_count, map_name)'.format(db_type))
        self.connection.commit()

    def _create_grid_data_tables(self, db_type):
        """Create grid data table for all lake-year combinations."""
        year = datetime.now().strftime('%Y')
        for lake in LAKES.values():
            self._create_grid_data_table(db_type=db_type, year=year, lake=lake)

    def _create_grid_data_table(self, db_type, year, lake):
        """Create NCAST or FCAST files table."""
        self.cursor.execute(
            'create table if not exists {}_{}_{}_grid_data '
            '(datetime, grid_number, map, lake, '
            'wave_height, wave_direction, wave_period, '
            'wind_speed, wind_direction, '
            'surface_temperature, '
            'current_speed, current_direction, '
            'ice_concentration, ice_thickness, ice_speed, ice_direction)'.format(lake, year, db_type))
        self.connection.commit()

    def create_map_file_tables(self):
        """Create a table for each map file."""
        # Loop through map files
        for filename in MAP_FILES:
            self._create_map_file_table(filename=filename)

    def _create_map_file_table(self, filename):
        """Create a table for a map file."""
        # Create map table
        self.cursor.execute(
            'create table if not exists {} (sequence_number, fortran_column, '
            'fortran_row, lat, lon, depth)'.format(filename.split('.')[0]))
        self.connection.commit()

        # Get map file
        map_file = NOAAMapFile(filename=filename)

        # Push to SQLite
        print('Pushing map data to SQL table...\n')
        map_file.map_data.to_sql(name=filename.split('.')[0], con=self.connection, if_exists='replace', index=False)

    def _create_surf_spot_table(self):
        """Create a table for a map file."""
        # Create map table
        self.cursor.execute('create table if not exists surf_spots (name, location, lake, lat, lon)')
        self.connection.commit()

        # Import CSV
        if os.path.isfile(os.path.join(DATA_DIR, 'surf_spots.csv')):

            # Import surf spots
            data = pd.read_csv(os.path.join(DATA_DIR, 'surf_spots.csv'))

            # Push to SQLite
            print('Pushing surf spots to SQL table...\n')
            data.to_sql(name='surf_spots', con=self.connection, if_exists='replace', index=False)

    def _df_to_table(self, df, db_type):
        """Insert DataFrame into SQLite table."""
        # Loop through DataFrame rows
        for idx in df.index:
            self._df_row_to_table(df_row=df.iloc[idx], db_type=db_type)

    def _df_row_to_table(self, df_row, db_type):
        """Insert DataFrame row into SQLite table."""
        # Select all rows from table
        self.cursor.execute('SELECT * FROM {}_files'.format(db_type).format(db_type))
        entry = self.cursor.fetchone()

        if entry is None:
            # If the table is empty
            self._df_row_to_table_insert(df_row=df_row, db_type=db_type)
        else:
            # Select rows with specified filename
            self.cursor.execute('select * from {}_files where filename=?'.format(db_type), (df_row['filename'],))
            entry = self.cursor.fetchone()

            if entry is None:
                # If file does not exist
                self._df_row_to_table_insert(df_row=df_row, db_type=db_type)
            else:
                pass

    def _df_row_to_table_insert(self, df_row, db_type):
        """Inset row to table."""
        values = (df_row['filename'], df_row['extension'], df_row['filetype'], df_row['lake'],
                  str(df_row['file_datetime']), str(df_row['current_datetime']), df_row['forecast'], df_row['url'])
        self.cursor.execute(
            'insert or ignore into {}_files values '
            '(null, null, ?, ?, ?, ?, ?, ?, ?, ?, null, null, null, null)'.format(db_type), values)
        self.connection.commit()

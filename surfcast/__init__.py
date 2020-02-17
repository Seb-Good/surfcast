# Import 3rd party libraries
import os

# Set working directory
WORKING_DIR = (
    os.path.dirname(
        os.path.dirname(
            os.path.realpath(__file__)
        )
    )
)

# Set data directory
DATA_DIR = os.path.join(WORKING_DIR, 'data')

# File types of interest [wave, wind, surface current, surface temperature, ice]
EXTENSIONS = {'wav': 'WAVES', 'wnd': 'WINDS', 'cur': 'SURFACE CURRENTS', 'swt': 'SURFACE TEMPS', 'ice': 'ICE PARAMS'}

# File attributes
FILE_ATTRIBUTES = {'WAVES': ['wave_height', 'wave_direction', 'wave_period'],
                   'WINDS': ['wind_speed', 'wind_direction'],
                   'SURFACE CURRENTS': ['surface_temperature'],
                   'SURFACE TEMPS': ['current_speed', 'current_direction'],
                   'ICE PARAMS': ['ice_concentration', 'ice_thickness', 'ice_speed', 'ice_direction']}

# Map files
MAP_FILES = ['superior10km.map', 'ontario5km.map', 'michigan2km.map', 'huron2km.map', 'erie2km.map']

# Lake names in the database
LAKES = {'e': 'erie', 'h': 'huron', 'o': 'ontario', 's': 'superior', 'm': 'michigan'}

# Set URL path to NOAA 'gridded fields' database
NOAA_URL = 'http://www.glerl.noaa.gov/ftp/EMF/glcfs/gridded_fields/'

# Set URL path to NOAA map files
MAP_URL = 'https://www.glerl.noaa.gov/emf/glcfs/gridded_fields/map_files/'

# Set map file attributes
MAP_ATTRIBUTES = [{'name': 'sequence_number', 'dtype': int},
                  {'name': 'fortran_column', 'dtype': int},
                  {'name': 'fortran_row', 'dtype': int},
                  {'name': 'lat', 'dtype': float},
                  {'name': 'lon', 'dtype': float},
                  {'name': 'depth', 'dtype': float}]

# Import python packages
from bs4 import BeautifulSoup
from urllib import request
import requests

# Set URL path to NOAA database
url = "http://www.glerl.noaa.gov/ftp/EMF/glcfs/gridded_fields/FCAST/"

# Get HTML from database page
html = requests.get(url)

# Create BeautifulSoup object
html_obj = BeautifulSoup(html.content)

# File types of interest [wave, wind, surface current, surface temperature]
file_types = ['wav', 'wnd', 'cur', 'swt']

# Define NOAA file class
class NoaaFile:

    # Initialize object
    def __init__(self, **kwargs):
        self.name = kwargs.get('name')
        self.extension = kwargs.get('extension')
        self.type = kwargs.get('type')
        self.date = kwargs.get('date')
        self.time = kwargs.get('time')
        self.size = kwargs.get('size')
        self.unit = 'MB'

        # File description
        if self.extension == 'wav':
            self.type = 'wave height and trajectory'
        elif self.extension == 'wnd':
            self.type = 'wind speed and trajectory'
        elif self.extension == 'cur':
            self.type = 'surface current'
        elif self.extension == 'swt':
            self.type = 'surface current'

# Create empty dictionary
files = []

# Extract Information
for link in html_obj.findAll('tr'):
    if len(link.contents) == 5 and link.contents[1].text.split('.')[-1] in file_types:
        files.append(NoaaFile(name=link.contents[1].text,
                              extension=link.contents[1].text.split('.')[-1],
                              date=link.contents[2].text.split(' ')[0],
                              time=link.contents[2].text.split(' ')[-1],
                              size=link.contents[3].text[0:len(link.contents[3].text)-1]))

# Download a file
def download_file(url, filename):

    # Join url and filename
    file_url = str(url + filename)

    # Send file request to server
    response = request.urlopen(file_url)

    # Parse test file by line breaks
    file = str(response.read()).split('\\n')

    return file

file = download_file(url, files[3].name)












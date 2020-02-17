# Define NOAA database class
class NoaaDB:
    
    """
    Class: NoaaDB
        - This class converts the NCAST|FCAST FTP database list into a pandas DataFrame
        
        - The gridded fields filename format is:

          LYYYYDDDHH.N.EXT

          L    = lake letter (s=Superior, m=Michigan, h=Huron, e=Erie, o=Ontario)
          YYYY = year at start of simulation (GMT)
          DDD  = Day Of Year at start of simulation (GMT)
          HH   = hr at start of simulation (GMT)
          N    = Site Number
    """

    # Initialize object
    def __init__(self, url, extension_list):
        
        # Set object attributes
        self.url = url                            # FTP 'gridded data' database URL
        self.url_ncast = self.url  + 'NCAST/'     # FTP NCAST database URL  
        self.url_fcast = self.url  + 'FCAST/'     # FTP FCAST database URL  
        self.url = url                            # FTP 'gridded data' database URL  
        self.extension_list = extension_list      # List of file type extensions   
        self.html_ncast = {}                      # HTML from FCAST page
        self.html_fcast = {}                      # HTML from FCAST page
        self.html_obj_ncast = {}                  # FCAST HTML Object
        self.html_obj_fcast = {}                  # FCAST HTML Object
        
        # Current time as UTC
        self.current_datetime_UTC = datetime.utcnow().replace(tzinfo=tz.gettz('UTC'))
        
        # Database dataframe
        self.df = pd.DataFrame(index=[], columns=['filename', 'file_extension', 
                                                  'filetype', 'lake', 'filedate', 
                                                  'filetime', 'file_datetime', 
                                                  'current_datetime', 'filesize', 'cast_type', 
                                                  'file_url'])                                     
        
    # Get NCAST database 
    def get_ncast(self):
        
        # Get DataFrame row count
        df_rows = self.df.shape[0]-1
        
        # Get HTML from database page
        self.html_ncast   = requests.get(self.url_ncast)
        
        # Create BeautifulSoup object
        self.html_obj_ncast   = BeautifulSoup(self.html_ncast.content)
        
        # Set database list as dataframe 
        for link in self.html_obj_ncast.findAll('tr'):
            if (
                len(link.contents) == 5 and 
                link.contents[1].text.split('.')[-1] in self.extension_list and 
                link.contents[1].text[0] in self.extension_list
               ):
                
                df_rows += 1  # row count                                                 
                
                filename = link.contents[1].text                                    # file name
                file_extension = link.contents[1].text.split('.')[-1]               # file extension
                filedate = link.contents[2].text.split(' ')[0]                      # file upload date
                filetime = link.contents[2].text.split(' ')[1]                      # file upload time 
                filesize = link.contents[3].text                                    # file size
                
                # Standardize file size to megabites
                if filesize[-1] == 'K':
                    filesize = str(float(filesize[0:len(filesize)-1])/1000.0) + 'M'

                # Convert data and time strings to a date|time object
                file_datetime = datetime.strptime(filedate + '-' + filetime, '%d-%b-%Y-%H:%M')
                file_datetime = file_datetime.replace(tzinfo=tz.gettz('US/Eastern'))
                
                # Set file type
                if file_extension == 'wav':           # Wave
                    filetype = 'WAVES'
                elif file_extension == 'wnd':         # Wind
                    filetype = 'WINDS'
                elif file_extension == 'cur':         # Surface Current
                    filetype = 'SURFACE CURRENTS'
                elif file_extension == 'swt':         # Surface Temperature
                    filetype = 'SURFACE TEMPS'
                elif file_extension == 'ice':         # Ice Conditions
                    filetype = 'ICE PARAMS'
                
                # Set great lake
                if filename[0] == 'e':       # Lake Erie
                    lake = 'erie'
                elif filename[0] == 'h':     # Lake Huron
                    lake = 'huron'
                elif filename[0] == 'o':     # Lake Ontario
                    lake = 'ontario'
                elif filename[0] == 's':     # Lake Superior
                    lake = 'superior'
                elif filename[0] == 'm':     # Lake Michigan
                    lake = 'michigan'       
                
                self.df.loc[df_rows] = [filename, file_extension, filetype, 
                                               lake, filedate, filetime, file_datetime, self.current_datetime_UTC, 
                                               filesize, 'NCAST', self.url_ncast]  # save to dataframe
                
    # Get FCAST database 
    def get_fcast(self):
        
        # Get DataFrame row count
        df_rows = self.df.shape[0]-1
        
        # Get HTML from database page
        self.html_fcast   = requests.get(self.url_fcast)
        
        # Create BeautifulSoup object
        self.html_obj_fcast   = BeautifulSoup(self.html_fcast.content)
        
        # Set database list as dataframe 
        for link in self.html_obj_fcast.findAll('tr'):
            if (
                len(link.contents) == 5 and 
                link.contents[1].text.split('.')[-1] in self.extension_list and 
                link.contents[1].text[0] in self.extension_list
               ):
                
                df_rows += 1  # row count                                                 
                
                filename = link.contents[1].text                                    # file name
                file_extension = link.contents[1].text.split('.')[-1]               # file extension
                filedate = link.contents[2].text.split(' ')[0]                      # file upload date
                filetime = link.contents[2].text.split(' ')[1]                      # file upload time 
                filesize = link.contents[3].text                                    # file size
                
                # Standardize file size to megabites
                if filesize[-1] == 'K':
                    filesize = str(float(filesize[0:len(filesize)-1])/1000.0) + 'M'
                
                # Convert data and time strings to a date|time object
                file_datetime = datetime.strptime(filedate + '-' + filetime, '%d-%b-%Y-%H:%M')
                file_datetime = file_datetime.replace(tzinfo=tz.gettz('US/Eastern')) 

                # Set file type
                if file_extension == 'wav':           # Wave
                    filetype = 'WAVES'
                elif file_extension == 'wnd':         # Wind
                    filetype = 'WINDS'
                elif file_extension == 'cur':         # Surface Current
                    filetype = 'SURFACE CURRENTS'
                elif file_extension == 'swt':         # Surface Temperature
                    filetype = 'SURFACE TEMPS'
                elif file_extension == 'ice':         # Ice Conditions
                    filetype = 'ICE PARAMS'
                
                # Set great lake
                if filename[0] == 'e':       # Lake Erie
                    lake = 'erie'
                elif filename[0] == 'h':     # Lake Huron
                    lake = 'huron'
                elif filename[0] == 'o':     # Lake Ontario
                    lake = 'ontario'
                elif filename[0] == 's':     # Lake Superior
                    lake = 'superior'
                elif filename[0] == 'm':     # Lake Michigan
                    lake = 'michigan'       
                
                self.df.loc[df_rows] = [filename, file_extension, filetype, 
                                               lake, filedate, filetime, file_datetime, self.current_datetime_UTC, 
                                               filesize, 'FCAST', self.url_ncast]  # save to dataframe


# Define NOAA database class for the newest file for a particular lake and attribute
class NoaaDB_NewestFile():
    
    """
    Class: NoaaDB_NewestFile
        - This class Takes a lake and attribute as input and finds the most recently uploaded corresponding file. 
    
    Inputs:
            - lake:       Lake of interest [ontario, michigan, erie, superior, huron] 
            - attribute:  File extension [cur, swt, wav, wnd] 
    """

    # Initialize object
    def __init__(self, noaa_files, lake, file_extension):
        
        # Set object attributes
        self.noaa_files = noaa_files              # user input NoaaDB object (Pandas Dataframe of all files in Noaa DB)
        self.lake = lake                          # user input lake
        self.file_extension = file_extension      # user input file extension (lake attribute)
        self.textfile_ncast = {}                  # downloaded ncast text file
        self.textfile_fcast = {}                  # downloaded fcast text file
        self.df_ncast = {}                        # downloaded ncast text file as DataFrame
        self.df_fcast = {}                        # downloaded fcast text file as DataFrame
        self.df = {}                              # Conbined NCAST and FCAST DataFrames of most recent 120 hr forecast
        
        # NCAST DataFrame
        self.df = pd.DataFrame(index=[], columns=['grid number', 'file_extension', 
                                                  'filetype', 'lake', 'filedate', 
                                                  'filetime', 'file_datetime', 
                                                  'current_datetime', 'filesize', 'cast_type', 
                                                  'file_url'])   
        
        
        # -------------------------------------------------------------------------------------------------------------------- #
        # NCAST
        # -------------------------------------------------------------------------------------------------------------------- #
        
        # Find index of newest file
        self.newest_index_ncast = self.noaa_files.df[(self.noaa_files.df.file_extension == self.file_extension) 
                                                     & (self.noaa_files.df.lake == self.lake)
                                                     & (self.noaa_files.df.forecast_type == 'NCAST')]['file_datetime'].idxmax()
        
        # Set object attributes from newest NCAST file
        self.filename_ncast         = self.noaa_files.df.iloc[self.newest_index_ncast].filename          # file name    
        self.filetype_ncast         = self.noaa_files.df.iloc[self.newest_index_ncast].filetype          # file type   
        self.file_datetime_ncast    = self.noaa_files.df.iloc[self.newest_index_ncast].file_datetime     # file UTC time   
        self.current_datetime_ncast = self.noaa_files.df.iloc[self.newest_index_ncast].current_datetime  # current UTC time 
        self.forecast_type_ncast    = self.noaa_files.df.iloc[self.newest_index_ncast].forecast_type     # file cast type
        self.file_url_ncast         = self.noaa_files.df.iloc[self.newest_index_ncast].file_url          # file url
        
        # -------------------------------------------------------------------------------------------------------------------- #
        # FCAST
        # -------------------------------------------------------------------------------------------------------------------- #
        
        # Find index of newest file
        self.newest_index_fcast = self.noaa_files.df[(self.noaa_files.df.file_extension == self.file_extension) 
                                                     & (self.noaa_files.df.lake == self.lake)
                                                     & (self.noaa_files.df.forecast_type == 'FCAST')]['file_datetime'].idxmax()
        
        # Set object attributes from newest NCAST file
        self.filename_fcast         = self.noaa_files.df.iloc[self.newest_index_fcast].filename          # file name    
        self.filetype_fcast         = self.noaa_files.df.iloc[self.newest_index_fcast].filetype          # file type  
        self.file_datetime_fcast    = self.noaa_files.df.iloc[self.newest_index_fcast].file_datetime     # file UTC time  
        self.current_datetime_fcast = self.noaa_files.df.iloc[self.newest_index_fcast].current_datetime  # current UTC time 
        self.forecast_type_fcast    = self.noaa_files.df.iloc[self.newest_index_fcast].forecast_type     # file cast type 
        self.file_url_fcast         = self.noaa_files.df.iloc[self.newest_index_fcast].file_url          # file url
        
        
        
        
    # Download newest file and save as row delimited list
    def download_file(self):

        """
        Function: download_file
            - This function will download the most recently uploaded file on the NOAA database that 
              corresponds to the lake and attribute input by the user.

        Outputs:
            - textfile_ncast:       The corresponding row delimited text file
            - textfile_fcast:       The corresponding row delimited text file
        """

        # -------------------------------------------------------------------------------------------------------------------- #
        # NCAST
        # -------------------------------------------------------------------------------------------------------------------- #            
        
        # Send file request to server and download
        response = request.urlopen(self.file_url_ncast + self.filename_ncast)

        # Parse text file by line breaks
        self.textfile_ncast = str(response.read()).split('\\n')
        
        # -------------------------------------------------------------------------------------------------------------------- #
        # FCAST
        # -------------------------------------------------------------------------------------------------------------------- #
        
        # Send file request to server and download
        response = request.urlopen(self.file_url_fcast + self.filename_fcast)

        # Parse text file by line breaks
        self.textfile_fcast = str(response.read()).split('\\n')
        
        
        
        
    # Convert NCAST and FCAST text files to individual dataframs     
    def create_dfs(self):
        
        """
        The gridded fields data format is:

        col_1 = grid number corresponding to a grid file
        
        For Wind Speed:
          col_2 = 10m wind speed at grid center (m/s)
          col_3 = wind direction at grid center (0 = from north, 90 = from east)

        For Surface Currents:
          col_2 = surface current speed at grid center (m/s)
          col_3 = surface current direction at grid center (0 = toward north, 90 = toward east)

        For Waves:
          col_2 = significant wave height at grid center (m)
          col_3 = significant wave direction at grid center (0 = toward north, 90 = toward east)
          col_4 = significant wave period (s)

        For Surface Water Temps:
           col_2 = surface water temperature (C) at grid center

        For Ice Model results:
           col_2 = ice concentration (0-1) at grid centerwww.f
           
           col_3 = ice thickness (m) at grid center
           col_4 = ice speed (m/s) at grid cente
           col_5 = ice direction (m/s) at grid center
        """
        
        # Extract header information  
        for row in range(len(file)):
            if files[file_num].filetype in file[row]:
                # Year, day, hour
                if file[row][0] == 'b':  
                    self.year = file[row][2:6]  # year
                    self.day = file[row][7:10]  # day of the year
                    print(self.year)
                    print(self.day)
                    print('')
                else: 
                    self.year = file[row][0:4]  # year
                    self.day = file[row][5:8]  # day of the year
                    print(self.year)
                    print(self.day)
                    print('')
        
        
        
        
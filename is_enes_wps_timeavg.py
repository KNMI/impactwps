from pywps.Process import WPSProcess

import icclim
import icclim.util.callback as callback
import logging
import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *
home = expanduser("~")
transfer_limit_Mb = 100

    
class ProcessSimpleIndice(WPSProcess):


    def __init__(self):
        WPSProcess.__init__(self,
                            identifier = 'is_enes_wps_timeavg', # only mandatary attribute = same file name
                            title = 'ICCLIM Time Averaging',
                            abstract = 'Computes time averages for any parameter by month, year of various seasons using ICCLIM. Please select the right variable and time range for the given input file. With this process it is for example possible to create yearly averages from daily data.  The result is written to your basket and can be used in new processes.',
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            grassLocation =False)


        self.sliceModeIn = self.addLiteralInput(identifier = 'sliceMode',
                                              title = 'Slice mode (temporal grouping to apply for calculations)',
                                              abstract = 'Selects temporal grouping to apply for calculations',
                                              type="String",
                                              default = 'None')
        self.sliceModeIn.values = ["None","year","month","ONDJFM","AMJJAS","DJF","MAM","JJA","SON"]


        self.filesIn = self.addLiteralInput(identifier = 'wpsnetcdfinput_files',
                                               title = 'Input filelist',
                                               abstract="The input filelist to calculate the mean values for. The inputs need to be accessible by opendap URL's. It is also possible to select from the basket a catalog containing multiple files. The catalog will then be expanded to multiple files.",
                                               type=type("S"),
                                               minOccurs=0,
                                               maxOccurs=1024,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc')
                                                
        self.varNameIn = self.addLiteralInput(identifier = 'wpsvariable_varName~wpsnetcdfinput_files',
                                               title = 'Variable name to process',
                                               abstract = 'Variable name to process as specified in your input files.',
                                               type="String",
                                               default = 'tasmax')
        

        self.timeRangeIn = self.addLiteralInput(identifier = 'wpstimerange_timeRange~wpsnetcdfinput_files', 
                                               title = 'A start/stop time range',
                                               abstract = 'Time range, e.g. 2010-01-01/2012-12-31. If None is selected, all dates in the file will be processed.',
                                               type="String",
                                               default = 'None')
        
        self.outputFileNameIn = self.addLiteralInput(identifier = 'wpsnetcdfoutput_outputFileName', 
                                               title = 'Name of output netCDF file',
                                               type="String",
                                               default = 'out_icclim.nc')
        
        
        self.NLevelIn = self.addLiteralInput(identifier = 'wpsnlevel_NLevel~wpsnetcdfinput_files', 
                                               title = 'Number of level (if 4D variable)',
                                               type="String",
                                               default = 'None')

        self.opendapURL = self.addLiteralOutput(identifier = "opendapURL",title = "opendapURL");   
        
    def callback(self,message,percentage):
        self.status.set("%s" % str(message),str(percentage));

    
    def execute(self):
        # Very important: This allows the NetCDF library to find the users credentials (X509 cert)
        tmpFolderPath=os.getcwd()
        os.chdir(home)
        
        def callback(b):
          self.callback("Processing",b)
         
        files = [];
        files.extend(self.filesIn.getValue())
        var = self.varNameIn.getValue()
        slice_mode = self.sliceModeIn.getValue()
        time_range = self.timeRangeIn.getValue()
        out_file_name = self.outputFileNameIn.getValue()
        level = self.NLevelIn.getValue()
        
        if(level == "None"):
            level = None
        
        if(slice_mode == "None"):
            slice_mode = None            
          
        if(time_range == "None"):
            time_range = None
        else:
            startdate = dateutil.parser.parse(time_range.split("/")[0])
            stopdate  = dateutil.parser.parse(time_range.split("/")[1])
            time_range = [startdate,stopdate]
     
        self.status.set("Preparing....", 0)
        
        pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        """ URL output path """
        fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        """ Internal output path"""
        fileOutPath = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        """ Create output directory """
        mkdir_p(fileOutPath)
        

        self.status.set("Processing input list: "+str(files),0)
        
        my_indice_params = {'indice_name': 'TIMEAVG',
                            'calc_operation': 'mean'
                           }
        
        
        from netCDF4 import Dataset
        dataset = Dataset(files[0]) 
        isMember = False
        memberIndex = 12
        try:
          for a in range(0,dataset.variables["member"].shape[0]):
            isMember = True
            logging.debug("Checking index " +str(a))
            logging.debug("Has value "+str(dataset.variables["member"][a]))
            memberValue = str("".join(dataset.variables["member"][a]))
            logging.debug(memberValue)
            if memberValue=="median":
              memberIndex=a
        except:
          pass
        
        if isMember == True:
          logging.debug("IS Member data")
          logging.debug("Using memberIndex "+str(memberIndex))
          icclim.indice(user_indice=my_indice_params, 
                          in_files=files,
                          var_name=var,
                          slice_mode=slice_mode,
                          time_range=time_range,
                          out_file=fileOutPath+out_file_name,
                          threshold=None,
                          N_lev=memberIndex,
                          lev_dim_pos=0,
                          #transfer_limit_Mbytes=transfer_limit_Mb,
                          callback=callback,
                          callback_percentage_start_value=0,
                          callback_percentage_total=100,
                          base_period_time_range=None,
                          window_width=5,
                          only_leap_years=False,
                          ignore_Feb29th=True,
                          interpolation='hyndman_fan',
                          out_unit='days')
        else:
          icclim.indice(user_indice=my_indice_params, 
                          in_files=files,
                          var_name=var,
                          slice_mode=slice_mode,
                          time_range=time_range,
                          out_file=fileOutPath+out_file_name,
                          threshold=None,
                          N_lev=level,
                          transfer_limit_Mbytes=transfer_limit_Mb,
                          callback=callback,
                          callback_percentage_start_value=0,
                          callback_percentage_total=100,
                          base_period_time_range=None,
                          window_width=5,
                          only_leap_years=False,
                          ignore_Feb29th=True,
                          interpolation='hyndman_fan',
                          out_unit='days')

        
        """ Set output """
        url = fileOutURL+"/"+out_file_name;
        self.opendapURL.setValue(url);
        self.status.set("ready",100);
        
        

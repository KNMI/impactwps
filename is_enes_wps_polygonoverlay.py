from pywps.Process import WPSProcess

import logging
import dateutil.parser
from datetime import datetime
import os
from os.path import expanduser
from mkdir_p import *
import base64
home = expanduser("~")
import ADAGUCFeatureFunctions
    
class Process(WPSProcess):
    def __init__(self):
        WPSProcess.__init__(                  self,
                                              identifier = 'is_enes_wps_polygonoverlay', # only mandatary attribute = same file name
                                              title = 'Polygon overlay',
                                              abstract = 'Polygon overlay function to calculate statistics for a gridded file by extracting geographical areas defined in a GeoJSON file. The statistics per geographical area include minimum, maximum, mean and standard deviation. The statistics are presented in a CSV table and a NetCDF file. Statistics can be calculated for several dates at once.',
                                              version = "1.0",
                                              storeSupported = True,
                                              statusSupported = True,
                                              grassLocation =False)


        self.wpsnetcdfinput_features = self.addLiteralInput(identifier = 'wpsnetcdfinput_features',
                                               title = 'Input File A - Polygon features',
                                               abstract="The input geojson, for example a NUTS GeoJSON file.",
                                               type=type("S"),
                                               minOccurs=1,
                                               maxOccurs=1,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/storyline_urbanheat/geojson/NUTS_2010_L0.geojson.nc')
        
        self.wpsnetcdfinput_data = self.addLiteralInput(identifier = 'wpsnetcdfinput_data',
                                               title = 'Input File B - Gridded data',
                                               abstract="Gridded input data on which the statistics per area are calculated.",
                                               type=type("S"),
                                               minOccurs=1,
                                               maxOccurs=1,
                                               default = 'http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc')
                                                
        self.wpsvariable_varName = self.addLiteralInput(identifier = 'wpsvariable_varName~wpsnetcdfinput_data',
                                               title = 'Variable name to process from file B',
                                               abstract = 'Variable name to process as specified in input File B.',
                                               type="String",
                                               default = 'tasmax')
        

        self.timeRangeIn = self.addLiteralInput(identifier = 'wpstimerange_timeRange~wpsnetcdfinput_data', 
                                               title = 'Time range from file B',
                                               abstract = 'Time range, e.g. 2010-01-01T12:00:00Z/2010-02-01T12:00:00Z. If * is given as input, all dates in the file will be processed.',
                                               type="String",
                                               default = '2010-02-22T12:00:00Z')
        
        
        self.bbox = self.addLiteralInput(      identifier = "bbox",
                                               title = "Bounding box",
                                               type="String",
                                               minOccurs=4,
                                               maxOccurs=4,
                                               default="-40,20,60,85",
                                               abstract="Specify The area of the grid to do the calculation for. Geographical units correspond to the coordinate reference system given at crs."
                                               )
        self.width  = self.addLiteralInput(     identifier = "width",
                                                title = "Width",
                                                type="String",minOccurs=1,maxOccurs=1,default="1500",
                                                abstract="Specify the width of the grid to do the calculation for")
        self.height = self.addLiteralInput(identifier = "height" ,title = "Height" ,type="String",minOccurs=1,maxOccurs=1,default="1500",abstract="Specify the height of the grid to do the calculation for")
        self.crs  = self.addLiteralInput(identifier = "crs"  ,title = "Coordinate reference system"  ,type="String",minOccurs=1,maxOccurs=1,default="EPSG:4326",abstract="The coordinate reference system of the grid to do the calculation in.")
        self.tags = self.addLiteralInput(identifier = "tags",title = "Your tag for this process",type="String",default="provenance_research_knmi");
        self.netcdfnutsstatfilename = self.addLiteralInput(identifier="netcdfnutsstatfilename",title = "NetCDF outputfile with geographical statistics",type="String",default="out_statistics.nc")
        self.csvnutsstatfilename = self.addLiteralInput(identifier="csvnutsstatfilename",title = "Output CSV filename", abstract = "Name of the comma separated value (CSV) output file. Will contain statistics in table form. Can be opened in a spreadsheet program.",type="String",default="out_statistics.csv")
        
        self.netcdfnutsstatout = self.addLiteralOutput(identifier = "netcdfnutsstatout",title = "Output NetCDF filename", abstract = "Name of the NetCDF filename. Will contain statistics in gridded form.",type="S");
        self.csvnutsstatout = self.addLiteralOutput(identifier = "csvnutsstatout",title = "CSV outputfile with statistics in table form", type="S");

        
        
    def callback(self,message,percentage):
        self.status.set("%s" % str(message),str(percentage));

    
    def execute(self):
        def callback(message,percentage):
            self.callback(message,percentage)
        tmpFolderPath=os.getcwd()
        os.chdir(home)


        self.status.set("Preparing....", 0)
        
        pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        """ URL output path """
        fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        """ Internal output path"""
        fileOutPath = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        """ Create output directory """
        mkdir_p(fileOutPath)
        self.status.set("Starting....", 1)
     
        bbox =  self.bbox.getValue()[0]+","+self.bbox.getValue()[1]+","+self.bbox.getValue()[2]+","+self.bbox.getValue()[3];
        time2 = self.timeRangeIn.getValue();
        width = int(self.width.getValue())
        height = int(self.height.getValue())

        CSV = ADAGUCFeatureFunctions.ADAGUCFeatureCombineNuts(
            featureNCFile = self.wpsnetcdfinput_features.getValue(),
            dataNCFile = self.wpsnetcdfinput_data.getValue(),
            variable = self.wpsvariable_varName.getValue(),
            bbox= bbox,
            time=time2,
            width=width,
            height=height,
            crs= self.crs.getValue(),
            outncfile=fileOutPath+self.netcdfnutsstatfilename.getValue(),
            outcsvfile=fileOutPath+self.csvnutsstatfilename.getValue(),
            callback=callback,
            tmpFolderPath=tmpFolderPath, 
            homeFolderPath=home)        

        #The final answer    
        self.netcdfnutsstatout.setValue(fileOutURL+"/"+self.netcdfnutsstatfilename.getValue());
        self.csvnutsstatout.setValue(fileOutURL+"/"+self.csvnutsstatfilename.getValue());
        self.status.set("Finished....", 100)      

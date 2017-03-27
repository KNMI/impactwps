from datetime import datetime

from pywps.Process import WPSProcess
from os.path import expanduser
import logging;
from mkdir_p import *

home = expanduser("~")
import urllib
import isodate 

import time
import netCDF4

from netCDF4 import MFDataset

import sys
from subprocess import PIPE, Popen
from threading  import Thread
import json
import os
import shutil

import iteratewcs


class Process(WPSProcess):
     def __init__(self):
        logging.debug("init");
        # init process
        WPSProcess.__init__(self,
                            identifier="WCS_subsetting", #the same as the file name
                            title="SpatialSelection",
                            version = "1.0",
                            storeSupported = True,
                            statusSupported = True,
                            abstract="Spatial extraction/subsetting.",
                            grassLocation =False)
        self.resource = self.addLiteralInput(identifier="resource",title = "Input file(s)",abstract="application/netcdf",type=type("S"),
                                             default="http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20760101-21001231.nc",minOccurs=0,maxOccurs=1024)
        self.outputFileName = self.addLiteralInput(identifier="outputFileName",title = "Output file name",type="String",default="wcs.nc")
        self.coverage = self.addLiteralInput(identifier = "coverage",title = "Coverage",type="String",default="tasmax")
        
        self.bbox = self.addLiteralInput(identifier = "bbox",title = "Bounding box in defined coordinate system",type="String",minOccurs=4,maxOccurs=4,default="-180,-90,180,90")
        self.crs = self.addLiteralInput(identifier = "crs",title = "Coordinate reference system",type="String",default="EPSG:4326")
        self.dates = self.addLiteralInput(identifier = "dates",title = "Start/stop/resolution in ISO8601 format",type="String",default="2076-01-01T12:00:00Z/2076-02-01T12:00:00Z/P1D")
        self.resx = self.addLiteralInput(identifier = "resx",title = "X resolution",type="String",default="1")
        self.resy = self.addLiteralInput(identifier = "resy",title = "Y resolution",type="String",default="1")
        self.tags = self.addLiteralInput(identifier = "tags",title = "Your tag for this process",type="String",default="provenance_research_knmi");
        self.opendapURL = self.addLiteralOutput(identifier = "opendapURL",title = "opendapURL");
        self.outputFormat = self.addLiteralInput(identifier = 'outputFormat',
                            title = 'outputFormat',
                            type="String",
                            default = 'netcdf')
        self.outputFormat.values = ["netcdf","geotiff","aaigrid"]#,"gtiff","aaigrid"]
        
     def callback(self,message,percentage):
        self.status.set("Processing: [%s]" % message,percentage);
        
     def execute(self):
        def callback(a,b):
          self.callback(a,b)
        tmpFolderPath=os.getcwd()
        os.chdir(home)
        logging.debug("executing "+self.identifier);

        self.status.set("Preparing....", 0)

        fileList = self.resource.getValue()
        
        pathToAppendToOutputDirectory = "/WPS_"+self.identifier+"_" + datetime.now().strftime("%Y%m%dT%H%M%SZ")
        
        """ URL output path """
        fileOutURL  = os.environ['POF_OUTPUT_URL']  + pathToAppendToOutputDirectory+"/"
        
        """ Internal output path"""
        fileOutPath = os.environ['POF_OUTPUT_PATH']  + pathToAppendToOutputDirectory +"/"

        """ Create output directory """
        mkdir_p(fileOutPath)
        
        """ Get output filename """
        outputfile = self.outputFileName.getValue()
        
        #TIME = "2006-01-01T12:00:00Z/2007-01-01T12:00:00Z/PT24H";
        TIME = self.dates.getValue();
        BBOX = "-180,-90,180,90";
        BBOX = self.bbox.getValue()[0]+","+self.bbox.getValue()[1]+","+self.bbox.getValue()[2]+","+self.bbox.getValue()[3];
        CRS = self.crs.getValue();
        
        
        
        WCSURL = "source="+fileList[0]+"&SERVICE=WCS&";
        RESX=self.resx.getValue();
        RESY=self.resy.getValue();
        FORMAT = self.outputFormat.getValue();
        status = -1
        try:
          status = iteratewcs.iteratewcs(TIME=TIME,BBOX=BBOX,CRS=CRS,WCSURL=WCSURL,RESX=RESX,RESY=RESY,COVERAGE=self.coverage.getValue(),TMP=tmpFolderPath,OUTFILE=fileOutPath+"/"+outputfile,FORMAT=FORMAT,LOGFILE=tmpFolderPath+"/adagucerrlog.txt",callback=callback)
        except Exception as e:
          return "Iterate over WCS Failed:\n "+str(e).replace('&','&amp;')
          
        
        if(status != 0):
          message = "iteratewcs failed";
          try:
            f = open(tmpFolderPath+"/adagucerrlog.txt")
            message = message+f.read()
            f.close()
          except:
            pass
            
          
          return message;
        
        """ Set output """
        url = fileOutURL+"/"+outputfile;
        self.opendapURL.setValue(url);
        self.status.set("ready",100);
        
        

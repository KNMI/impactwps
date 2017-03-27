

from pywps.Process.Process import WPSProcess   

from os.path import expanduser
from mkdir_p import *
from datetime import datetime
import base64
from clipc_compatibilityqueries import clipc_compatibilityqueries

home = expanduser("~")

class Process(WPSProcess):
    def __init__(self):
        # init process
        WPSProcess.__init__(self,
                            identifier="clipc_combine_identify", #the same as the file name
                            title="CLIPC Combine Identify",
                            version = "1.0",
                            storeSupported = "true",
                            statusSupported = "true",
                            abstract="Lists possible operations for two resources for the CLIPC Combine processor.",
                            grassLocation =False)
        
        self.inputa = self.addLiteralInput(identifier="inputa",
                                                title="Input 1",
                                                abstract="application/netcdf",
                                                default = "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/jrc/ano_FAPARMean_pheno_JRC_1.0_EC-JRC_FAPAR_JRC_yr_19980101-20111231_1998-2011.nc",
                                                type = type("String"))   
        self.inputb = self.addLiteralInput(identifier="inputb",
                                                title="Input 2",
                                                abstract="application/netcdf",
                                                default = "http://opendap.knmi.nl/knmi/thredds/dodsC/CLIPC/tudo/tier3/waterb_arcgis-10-4-0_IRPUD_JRC-LUISA-Landuse_10yr_20100101-20501231.nc",
                                                type = type("String"))   
       
        
        self.result = self.addLiteralOutput(identifier = "result",title = "answer");
        self.inputorder = self.addLiteralOutput(identifier = "inputorder",title = "inputorder");
        self.compatibilityquerymessage = self.addLiteralOutput(identifier = "compatibilityquerymessage",title = "The compatibility query result");
        self.compatibilityquerynroferrors = self.addLiteralOutput(identifier = "compatibilityquerynroferrors",title = "Number of errors found for the compatibility query result");

    
    def execute(self):
        
        self.result.setValue("{\"operator\":[\"add\",\"substract\",\"divide\",\"multiply\"]}");
        self.inputorder.setValue("input1,input2,time1,time2,norm1,norm2,bbox,operator,width,height,outputfilename");

        checker = clipc_compatibilityqueries()

        checker.testfiles(self.inputa.getValue(),
                          self.inputb.getValue())
        
        self.compatibilityquerymessage.setValue("base64:"+base64.b64encode(checker.getMessages()));
        self.compatibilityquerynroferrors.setValue(checker.getNumErrors());

        self.status.set("Finished....", 100)      

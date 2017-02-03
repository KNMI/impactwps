import iteratewcs
def test():  
  def callback(m,p):
    print m+" ("+str(p)+" %)"
    
  TIME = "2006-01-01T12:00:00Z/2007-02-01T12:00:00Z/PT24H";
  BBOX = "-179.4375,-89.702158,180.5625,89.7021580";
  CRS = "EPSG:4326";
  WCSURL = "source=http://opendap.knmi.nl/knmi/thredds/dodsC/IS-ENES/TESTSETS/tasmax_day_EC-EARTH_rcp26_r8i1p1_20060101-20251231.nc&SERVICE=WCS&";
  COVERAGE="pr"
  COVERAGE="tasmax"
  RESX=1.125;
  RESY=1.121276975;
  
  
  iteratewcs.iteratewcs(TIME=TIME,BBOX=BBOX,COVERAGE=COVERAGE,CRS=CRS,WCSURL=WCSURL,RESX=RESX,RESY=RESY,LOGFILE="iteratewcs.txt",callback=callback,OUTFILE="iteratewcs.nc")



test()

import urllib
import isodate 
import time
import netCDF4
from netCDF4 import MFDataset
import sys
from subprocess import PIPE, Popen, STDOUT
from threading  import Thread
import json
import os
import shutil
from mkdir_p import *
import zipfile
from xml.sax.saxutils import escape

class CGIRunner:
  
  def __init__(self):
    self.headersSent = False
    self.foundLF = False
    
 
  def startProcess(self,cmds,callback=None,env = None,bufsize=0):
    try:
        from Queue import Queue, Empty
    except ImportError:
        from queue import Queue, Empty  # python 3.x

    ON_POSIX = 'posix' in sys.builtin_module_names

    def enqueue_output(out, queue):
        for line in iter(out.readline, b''):
            queue.put(line)
        out.close()


    p = Popen(cmds, stdout=PIPE, stderr=STDOUT,bufsize=bufsize, close_fds=ON_POSIX,env=env)
    q = Queue()
    t = Thread(target=enqueue_output, args=(p.stdout, q))
    t.daemon = True # thread dies with the program
    t.start()

    #http://stackoverflow.com/questions/156360/get-all-items-from-thread-queue
    # read line without blocking
    while True:
      #time.sleep(100/1000000.0)
      #time.sleep(0.01)
      try:  
        line = q.get(timeout=.01)
        if(callback != None):
          callback(line)
      except Empty:
          if(t.isAlive() == False):
            break;
    
    """ Sometimes stuff is still in que """
    
    while True:
      #time.sleep(100/1000000.0)
      time.sleep(0.1)
      try:  
        line = q.get(timeout=.1)
        if(callback != None):
          callback(line)
      except Empty:
          if(t.isAlive() == False):
            break;
  
    return p.wait()
  
  def filterHeader(self,_message,writefunction):
      if self.headersSent == False:
        message = bytearray(_message)
        endHeaderIndex = 0
        for j in range(len(message)):
          if message[j] == 10 :
            if self.foundLF == False:
              self.foundLF = True
              #print "LF Found"
              continue
          elif self.foundLF == True and message[j] != 13:
            self.foundLF = False
            #print "Sorry, not LF Found"
            continue
          
          if(self.foundLF == True):  
            if message[j] == 10 :
              #print "Second LF Found"
              self.headersSent = True;
              endHeaderIndex = j+2;
              #print "HEADER FOUND"
              #print message[:endHeaderIndex]
              writefunction(message[endHeaderIndex:])
              
              break;
      else:
        writefunction(_message)
 
 
  """
    Behaves like a dynamic webserver. Run executables in webserver environment and can strip the headers from the response
    isLocalADAGUC sets ADAGUC_WRITETOFILE where the results will be written, normaly ADAGUC writes to stdout. 
  """  
  def run(self,cmds,url,out,extraenv = [], isLocalADAGUC = False):
    try:
      os.remove(out)
    except:
      pass
    
    if(isLocalADAGUC == True):
      env = os.environ.copy()
      env['QUERY_STRING']=url
      env['ADAGUC_WRITETOFILE']=out
      env.update(extraenv)  
      from subprocess import call
      status = call(cmds, stdin=None, stdout=None, stderr=None, shell=False, env=env)
    else:
      ncout = open(out,"a+b")
    
      self.headersSent = False
      self.foundLF = False
      
      def writefunction(data):
        ncout.write(data)
      
      def monitor1(_message):
        self.filterHeader(_message,writefunction)
      
      env = os.environ.copy()
      env['QUERY_STRING']=url
      env.update(extraenv)  
      status = self.startProcess(cmds,monitor1,env,bufsize=8192)
      ncout.close()
      
    return status

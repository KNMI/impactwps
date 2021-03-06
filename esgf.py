"""
BSD Licence
Copyright (c) 2009, Science & Technology Facilities Council (STFC)
All rights reserved.

Patch pydap to enable ESGF security.

@author: Stephen Pascoe <Stephen.Pascoe@stfc.ac.uk>

"""

import urllib2
import re
import os, sys
import socket
import M2Crypto

import pydap.lib
from pydap.exceptions import ClientError

import logging
log = logging.getLogger('esgf.security')

def install_esgf_client(certfile, keyfile):
    """
    Based on the CAS authentication example at
    http://pydap.org/client.html#authentication

    This function will patch the pydap package to provide a client
    certificate for HTTPS calls.

    :param certfile: filename of a client certificate or certificate
        chain in PEM format.
    :param keyfile: filename of a private key in PEM format.

    """

    # Create HTTPSHandler    
    ssl_context = M2Crypto.SSL.Context()
    ssl_context.load_cert_chain(certfile, keyfile)
    opener = M2Crypto.m2urllib2.build_opener(ssl_context, 
                                             urllib2.HTTPCookieProcessor,
                                             urllib2.ProxyHandler)

    opener.addheaders = [('User-agent', pydap.lib.USER_AGENT)]
    urllib2.install_opener(opener)

    def new_request(url):
        log.debug('Opening URL %s' % url)
        
        maxretries = 10;
        timeout = 60;
        while maxretries >0:
          def new_request_retry(maxretries,e):
              
              maxretries = maxretries - 1
              if(maxretries <= 0):
                  raise e
              #print ("Exception [%r] received, retries left %d: "%(e, maxretries))
              log.debug("Exception [%r] received, retries left %d: "%(e, maxretries))

              return maxretries
          try:
              r = urllib2.urlopen(url.rstrip('?&'),timeout=timeout)
              resp = r.headers.dict
              resp['status'] = str(r.code)
              data = r.read()
              # Ok
              maxretries = 0;
#          except urllib2.URLError as e:
              # General exception
#              if isinstance(e.reason, socket.timeout):
#                 maxretries = new_request_retry(maxretries,e)
#            else:
#                maxretries = new_request_retry(maxretries,e)
                # Enable the following lines to retry only when a timeout occurs, otherwise retry is done on any exception.
                #maxretries = 0;
                #raise
          except socket.timeout as e:
              maxretries = new_request_retry(maxretries,e)
          except:
              e = sys.exc_info()[0]
              maxretries = new_request_retry(maxretries,e)

        # When an error is returned, we parse the error message from the
        # server and return it in a ``ClientError`` exception.
        if resp.get("content-description") == "dods_error":
            m = re.search('code = (?P<code>\d+);\s*message = "(?P<msg>.*)"',
                    data, re.DOTALL | re.MULTILINE)
            msg = 'Server error %(code)s: "%(msg)s"' % m.groupdict()
            raise ClientError(msg)

        return resp, data

    from pydap.util import http
    http.request = new_request



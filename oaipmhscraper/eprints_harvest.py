#!/usr/bin/env python
# -*- coding: utf-8 -*-

import simplejson

from oaipmh_scraper import OAIPMHScraper

from eprintsxml import Eprints3XML, NS
from xml.parsers.expat import ExpatError

from datetime import datetime

import sys, traceback
import logging

logger = logging.getLogger("Eprints3 Harvester")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)

Redis = False

try:
    from redis import Redis
except ImportError:
    logger.warn("Cannot load a redis client - the file downloads will have to be done synchronously if selected.")
    from urllib import urlencode, urlopen

class Eprints3Harvester(OAIPMHScraper):
    def __init__(self, storage_dir, base_oai_url=None, identifier_uri_prefix=None, download_queue="q:download_list", async=True):
        try: 
            # This shouldn't fail... but...
            OAIPMHScraper.__init__(self, storage_dir, base_oai_url, identifier_uri_prefix)
        except AttributeError:
            pass
        if Redis:
            self.async = async # allow user to overwrite this
        if self.async:
            self.r = Redis()
            self.download_queue = download_queue
            logger.info("Download tasks will be queued asynchronously in %s" % download_queue)
            self.handle_file = self._queue_file
        else:
            self.handle_file = self._get_file

    def _queue_file(self, pid, main, url, filename):
        msg={'pid':pid, 'silo':self.store.state['storage_dir'],
             'url':url, 'filename':filename}
        jmsg = simplejson.dumps(msg)
        self.r.push(self.download_queue, jmsg)
        logger.info("%s queued for download" % jmsg)
        
    def _get_file(self, pid, main, url, filename):
        obj = self.store.get_item(pid)
        fromtime = datetime.now()
        logger.info("Starting download of %s (from %s) to object %s" % (filename, url, pid))
        try:
            obj.put_stream(filename, urlopen(url))
            totime = datetime.now()
            logger.info("Download completed in %s seconds" % ((totime-fromtime).seconds))
        except Exception, e:
            logger.error("Error downloading %s for pid:%s" % (url, obj.item_id))
            logger.error("Exception: %s" % (e))
    
    def getRecords(self, metadataPrefix="XML", update=True, _from=None, _until=None, template="%(pid)s/%(prefix)s/mieprints-eprint-%(pid)s.xml"):
        if metadataPrefix not in self.getMetadataPrefixes():
            # May not be a field that is available through the OAIPMH service, but may be
            # as a record export from each individual record - will try to download them separately
            eprints_cgi_root = ""
            if self.state['identify']['baseURL'].endswith("oai2"):
                eprints_cgi_root = self.state['identify']['baseURL'][:-4] + "export/" # ... /cgi/oai2
            elif self.state['identify']['baseURL'].endswith("oai2/"):
                eprints_cgi_root = self.state['identify']['baseURL'][:-5] + "export/" # ... /cgi/oai2/
            elif template.startswith("http://"):
                logger.info("Couldn't deduce export url from oai endpoint - using the template as it stands")
            else:
                logger.error("Couldn't work out the export URL for this endpoint")
                raise Exception
                
            for ident, date in self.getIdentifiers(update):
                obj = self.store.get_item(ident)
                if metadataPrefix == "XML":
                        obj.metadata['eprintsxml'] = metadataPrefix
                        obj.sync()
                if metadataPrefix not in obj.files or update:
                    url = eprints_cgi_root + template % {'prefix':metadataPrefix, 'pid':ident.split(":")[-1]}
                    logger.info("Adding %s for download for item %s" % (url, ident))
                    self.handle_file(obj.item_id, 
                                     "", 
                                     url ,
                                     metadataPrefix)
        else:

            return OAIPMHScraper.getRecords(self, metadataPrefix, update, _from, _until)
    
    def reprocessRecords(self, redownload_files=False):
        for item in self.store.list_items():
            obj = self.store.get_item(item)
            ep3_filename = "ep3"
            if obj.metadata.has_key('eprintsxml'):
                ep3_filename = obj.metadata['eprintsxml']
            with obj.get_stream(ep3_filename) as ep3text:
                ep3xml = Eprints3XML(ep3text.read())
                for (main, url, filename) in self._get_file_list(ep3xml):
                    if (filename not in obj.files) or redownload_files:
                        self.handle_file(obj.item_id, main, url, filename)

    def _get_file_list(self, ep3xml):
        for document in ep3xml.get_documents():
            main = document['main']
            for file_item in document['files']:
                url = file_item['url']
                filename = file_item['filename']
                yield (main, url, filename)

    def postprocessRecord(self, pid, text):
        try:
            e = Eprints3XML(text)
            for (main, url, filename) in self._get_file_list(e):
                self.handle_file(pid, main, url, filename)
        except ExpatError,e:
            logger.error("The text for item %s couldn't be parsed by the XML reader.")

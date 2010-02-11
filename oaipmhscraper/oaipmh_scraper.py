#!/usr/bin/env python
# -*- coding: utf-8 -*-

# OAIPMH Scraper

from os import path, mkdir

import simplejson

from oaipmh.client import Client as OaipmhClient

from oaipmh.metadata import global_metadata_registry

from datetime import datetime

import logging

import sys, traceback

from lxml import etree

logger = logging.getLogger("OAIPMH Harvester")
logger.setLevel(logging.INFO)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
ch.setFormatter(formatter)

logger.addHandler(ch)

from recordsilo import Silo, HarvestedRecord, PersistentState

def mdprefixcheck(target):
    def wrapper(self, *args, **kwargs):
        if not self.state.has_key("metadataPrefixes"):
            self.getMetadataPrefixes()
        return target(self, *args, **kwargs)
    return wrapper

class OAIPMHScraper(object):
    """Class to scrape the metadata from an OAIPMH target (given a metadataPrefix) and store the resultant sections in a Pairtree store as XML files. To get more than this, it is expected to subclass this class and overwrite the 'process(...)' method through which all the sections pass through."""
    def __init__(self, storage_dir, base_oai_url=None, identifier_uri_prefix=None):
        self.store = Silo(storage_dir, uri_base=base_oai_url, 
                          base_oai_url=base_oai_url, identifier_uri_prefix=identifier_uri_prefix)
        self.state = PersistentState(storage_dir, "oaipmh_harvester.json")
        self._init_clients()
        
    def logactivity(self, **kw):
        try:
            jsonmsg = simplejson.dumps(kw)
            logger.debug(jsonmsg)
            return jsonmsg
        except:
            logger.info("Failed to serialise as JSON using simplejson: %s" % msg)

    def _init_clients(self):
        try:
            self._c = OaipmhClient(self.store.state['base_oai_url']) #, metadata_registry = dumbMetadataRegistry)
            self.identify()
        except OSError:
            logger.error("Cannot make OAIPMH client")
            raise Exception("Cannot make OAIPMH client")

    def identify(self, refresh_cache=False):
        if self.state.has_key("identify") and not refresh_cache:
            return self.state['identify']
        else:
            i = self._c.handleVerb("Identify", {})
            identify = {}
            identify['repositoryName'] = i._repositoryName
            identify['baseURL'] = i._baseURL
            identify['protocolVersion'] = i._protocolVersion
            identify['adminEmails'] = i._adminEmails
            identify['earliestDatestamp'] = str(i._earliestDatestamp)
            identify['deletedRecord'] = i._deletedRecord
            identify['granularity'] = i._granularity
            identify['compression'] = i._compression
            identify['descriptions'] = i._descriptions
            self.state['identify'] = identify
            self.state['lastidentified'] = datetime.now().isoformat()
            self.state.sync()
            return self.state['identify']

    def getMetadataPrefixes(self, refresh_cache=False):
        if self.state.has_key("metadataPrefixes") and not refresh_cache:
            return self.state['metadataPrefixes']
        else:
            metadataPrefixes = self._c.handleVerb("ListMetadataFormats", {})
            self.state['metadataPrefixes'] = dict([ (a,(b,c)) for (a,b,c) in metadataPrefixes])
            self.state['lastcheckmetadataformats'] = datetime.now().isoformat()[:19] # YYYY-mm-DDTHH:MM:ss
            self.state.sync()
            return self.state['metadataPrefixes']

    def getIdentifiers(self, update=False):
        args = {'metadataPrefix':'oai_dc'}
        if not self.state.has_key('harvests'):
            self.state['harvests'] = []
        self.state['harvests'].append(datetime.now().isoformat()[:19])   # YYYY-mm-DDTHH:MM:ss
        self.state.sync()
        for header in self._c.handleVerb("ListIdentifiers", args):
            pid = header.identifier()
            date=header.datestamp().isoformat()
            logger.info("Found identifier %s - adding header metadata to harvested record" % pid)
            obj = self.store.get_item(pid, date)
            obj.metadata['identifier'] = pid
            obj.metadata['firstSeen'] = date
            obj.metadata['setSpec'] = header.setSpec()
            if header.isDeleted():
                obj.metadata['deleted_at_version'] = obj.currentversion
                obj.metadata['deleted_at_date'] = date
                logger.info("Object with identifier: %s has an isDeleted flag." % (pid))
            yield (pid, date)

    @mdprefixcheck
    def getRecords(self, metadataPrefix=None, update=False, _from=None, _until=None):
        if metadataPrefix in self.state['metadataPrefixes']:
            # if not global_metadata_registry.hasReader(metadataPrefix):
            class DumbReader(object):
                def __call__(self, element):
                    return element
            global_metadata_registry.registerReader(metadataPrefix, DumbReader())
            args = {'metadataPrefix':metadataPrefix}
            if _from:
                args['from'] = _from
            if _until:
                args['until'] = _until
            if not self.state.has_key('harvests'):
                self.state['harvests'] = []
            if update and self.state['harvests']:
                args['from'] = self.state['harvests'][-1]
            self.state['harvests'].append(datetime.now().isoformat()[:19])   # YYYY-mm-DDTHH:MM:ss
            self.state.sync()
            i = self._c.handleVerb("ListRecords", args)
            for header, element, _ in i:
                text = unicode(etree.tostring(element)).encode('UTF-8') # cast to unicode and encode to ('utf-8')
                pid = header.identifier()
                date=header.datestamp().isoformat()
                text = self.preprocessRecord(pid, text)
                obj = self.store.get_item(pid, date)
                if obj.files:
                    logger.info("Update to object: %s found with datestamp %s - storing update." % (pid,date))
                    obj.increment_version(date)
                    if header.isDeleted():
                        obj.metadata['deleted_at_version'] = obj.currentversion
                        obj.metadata['deleted_at_date'] = date
                        logger.info("Object with identifier: %s found with datestamp %s - storing." % (pid,date))
                    else:
                        obj.put_stream(metadataPrefix, text, metadata=True)
                else:
                    logger.info("New object: %s found with datestamp %s - storing." % (pid,date))
                    obj.metadata['identifier'] = pid
                    obj.metadata['firstSeen'] = date
                    obj.metadata['setSpec'] = header.setSpec()
                    if header.isDeleted():
                        obj.metadata['deleted_at_version'] = obj.currentversion
                        obj.metadata['deleted_at_date'] = date
                        logger.info("Object with identifier: %s found with datestamp %s - storing." % (pid,date))
                    else:
                        obj.put_stream(metadataPrefix, text, metadata=True)
                obj.sync()
                self.postprocessRecord(pid, text)
        else: # no such metadata prefix
            logger.error("No such metadataprefix available from the endpoint - try one of: %s " % self.state['metadataPrefixes'].keys())

    def preprocessRecord(self, pid, text):
        """This is called before anything is done with the scraped OAI-PMH record. It must return the xml text
        of the record that is wished to be stored."""
        return text

    def postprocessRecord(self, pid, text):
        """This method is called after the record is stored in the Silo. This would be the perfect method to
        override to enable you to read the record, and work out which additional resources are needing to be stored
        and queue them for download"""
        pass

# -*- coding: utf-8 -*-

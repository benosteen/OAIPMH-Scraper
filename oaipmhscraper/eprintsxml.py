#!/usr/bin/env python
# -*- coding: utf-8 -*-

import xml.etree.ElementTree as ET

from BeautifulSoup import BeautifulStoneSoup
NS = {'oai':u"{http://www.openarchives.org/OAI/2.0/}",
      'ep':u"{http://eprints.org/ep2/data/2.0}"}

class NoEP3XMLParsed(Exception):
    pass

def gotep3xml(target):
    def wrapper(self, *args, **kwargs):
        if self._et != None:
            return target(self, *args, **kwargs)
        else:
            raise NoEP3XMLParsed
    return wrapper

class Eprints3XML(object):
    def __init__(self, ep3_text=None):
        self._et = None
        self.id = None
        if ep3_text:
            self._parse_tree(ep3_text)

    def set_epxml(self, ep3_text):
        self._parse_tree(ep3_text)

    def _parse_tree(self, ep3_text):
        if isinstance(ep3_text, unicode):
            ep3_text = ep3_text.encode('utf-8')
        try:
            self.ep3_text = unicode(BeautifulStoneSoup(ep3_text))
            _et = ET.fromstring(self.ep3_text.encode("utf-8"))
            if _et.tag == NS['ep'] + "eprint":
                self._et = _et
            else:
                if _et.find(NS['ep'] + "eprint"):
                    self._et = _et.find(NS['ep'] + "eprint")
                else:
                    raise Exception
        except Exception, e:
            print "Error parsing the Eprint3 XML"
            print e

    @gotep3xml
    def get_id(self):
        return self._et.find(NS['ep']+u"eprintid").text

    @gotep3xml
    def get_creators(self):
        for item in self._et.find(NS['ep']+u"creators").getchildren():
            item_details = {}
            for field in item.getchildren():
                if field.tag == NS['ep']+u"name":
                    for namepart in field.getchildren():
                        item_details[namepart.tag[len(NS['ep']):]+"name"] = namepart.text
                else:
                    item_details[field.tag[len(NS['ep']):]] = field.text
            yield item_details

    @gotep3xml
    def get_documents(self):
        for doc in self._et.find(NS['ep']+u"documents").getchildren():
            doc_details = {}
            for field in doc.getchildren():
                if field.tag == NS['ep']+u"files":
                    doc_details['files'] = []
                    for file_section in field.getchildren():
                        file_details = {}
                        for file_field in file_section.getchildren():
                            file_details[file_field.tag[len(NS['ep']):]] = file_field.text
                        doc_details["files"].append(file_details)
                else:
                    doc_details[field.tag[len(NS['ep']):]] = field.text
            yield doc_details

    @gotep3xml
    def list_fields(self):
        #root = self._et.find(NS['ep']+u"eprint")
        #if root != None:
        #    return [x.tag[len(NS['ep']):] for x in root.getchildren()]
            return [x.tag[len(NS['ep']):] for x in self._et.getchildren()]

    @gotep3xml
    def get(self, tag_name):
        tag_name = ("/%s" % NS['ep']).join(tag_name.split("/"))
        tag_element = self._et.find(NS['ep']+tag_name)
        if tag_element != None:
            if tag_element.text.strip():
                return tag_element.text
            else:
                return tag_element
        else:
            return None

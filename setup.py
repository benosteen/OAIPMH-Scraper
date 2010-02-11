from ez_setup import use_setuptools
use_setuptools()
from setuptools import setup, find_packages

setup(name="OAIPMHScraper",
      version="0.2",
      description="OAIPMH Scraper - grabs and sync's metadata from an OAIPMH target and stores it locally in a Pairtree structure.",
      long_description="OAIPMH Scraper - grabs and sync's metadata from an OAIPMH target and stores it locally in a Pairtree structure.",
      author="Ben O'Steen",
      author_email="bosteen@gmail.com",
      scripts = ['bin/oaipmh_file_downloader'],
      packages=find_packages(exclude='tests'),
      install_requires=['pairtree', 'simplejson', 'pyoai', 'recordsilo'],
      )

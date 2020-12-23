#!/home/ec2-user/dev/python_env/bin/python

# coding: utf8
import getopt
import sys
import logging
from google.cloud import firestore
import pandas as pd
import mySecrets
secrets = mySecrets.getSecrets()
import json
import myInit
myInit.setup_logging()
logger = logging.getLogger()
config = myInit.getConfig()
db = firestore.Client()

class Search(object):
    Collection=firestore.Client().collection('searches')
    Batch=db.batch()
    NotSaved=0
    def __init__(self, label, results=[]):
        self.label = label
        self.results = results

    @staticmethod
    def from_dict(source):
        search = Search(source[u'label'])
        if u'results' in source:
            search.results = source[u'results']
        return search

    def to_dict(self):
        dest = {
            u'label': self.label,
        }
        if self.results:
            dest[u'results'] = self.results
        return dest
    def commitBatch():
        if Search.NotSaved > 0:
            Search.Batch.commit()
            Search.Batch = db.batch()
    def __repr__(self):
        return(
            f'Search(\
                label={self.label}, \
                results={self.results}, \
            )'
        )
    def createOne(self):
        Collection.add(search.to_dict())
    def createBatch(self):
        Search.Batch.set(self.Collection.document(), self.to_dict())
        Search.NotSaved=Search.NotSaved+1
        if Search.NotSaved == 100:
            self.commitBatch()
            Search.NotSaved=0
    def read(self):
        found=False
        query = self.Collection.where(u'label', u'==', self.label).stream()
        for doc in query:
            s=doc.to_dict()
            self.results = s["results"]
            found=True
            break
        if not found:
            self.results="NOTFOUND"



def initBufferFromPickle():

    bufferFile = config["buffers"]["search"]
    logger.debug(bufferFile)

    bufferSearch = pd.read_pickle(bufferFile, compression='gzip')
    logger.debug(bufferSearch.columns)
    batch = db.batch()

    for index, row in result.iterrows():
        s = db.collection(u'searches').document(str(index))
        search = Search(label=row["denomination"], results=row["search"])
        batch.set(s, search.to_dict())
        if index % 400 == 0:
            batch.commit()
            batch = db.batch()

    batch.commit()


s=Search("CIE BOIS DERIVES GEVAUDAN")
s.read()
logger.debug(s)
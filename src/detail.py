#!/home/ec2-user/dev/python_env/bin/python
# coding: utf8
import logging
import myLog
logger = logging.getLogger(__name__)
import pandas as pd
import sys
import getopt
import os.path
import configparser
from pathlib import Path
import config
pathDataFiles=config.paths["data"]
import glob
from tabulate import tabulate
pdtabulate=lambda df:tabulate(df,headers='keys',tablefmt='psql')


logging.basicConfig(level=logging.DEBUG, format='%(asctime)s %(name)s %(levelname)s:%(message)s')
logger = logging.getLogger(__name__)

 
def usage(n):
    print ("Usage : %s -s <siren> -f>"%n)

def details(df):
    print("----")
    print (pdtabulate(df[["siren", "activiteUnite", "siret","activiteEtablissement", "denomination", "region", "commune", "groupEffectifEtablissement","ownedDomainFinal"]].sort_values('groupEffectifEtablissement')))
    print("----")
    logger.debug("end details")

def details_full(df):
    print("----")

    print(pdtabulate(df))
    print(*df["domains"],sep = "\n")
    print(*df["search"],sep = "\n")
    print(*df["titleDomain"],sep = "\n")

    

    print("----")
    logger.debug("end details")




try:
    opts, _ = getopt.getopt(sys.argv[1:],'s:l:f')
except getopt.GetoptError :
        usage(sys.argv[0])
        sys.exit(2)
siren = 0
numeric_level = logging.INFO
full=False                                                
for opt, arg in opts:
        if opt  in ('--siren', "-s") :
            siren = int(arg)
        if opt in  ("--log", "-l"):
            numeric_level = getattr(logging, arg.upper(), None)
            if not isinstance(numeric_level, int):
                raise ValueError('Invalid log level: %s' % loglevel)
            else:
                print("on met à jour")
                #logger.setLevel(numeric_level)
        if opt in ("-f"):
            full=True

pattern=pathDataFiles + "/*.data" 
logger.debug("pattern : %s", pattern)
dataFiles = [f for f in glob.glob(pattern)]
#logger.debug("dataFiles : %s", dataFiles)

pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option('expand_frame_repr', False)

for file in dataFiles:
    df= pd.read_json(file, orient='index')
    if siren !=0:
        if len(df.loc[df["siren"]==siren]):
            print("Fichier : %s", file)
            if full== False:
                details(df[df["siren"] == siren])
            else:
                details_full(df[df["siren"] == siren])
            #sys.exit(0)
    else:
        if df.shape[0] > 0:
            print("Fichier : %s", file)
            if full==False:
                details(df)
            else:
                details_full(df)




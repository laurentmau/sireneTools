#!/home/ec2-user/dev/python_env/bin/python
# coding: utf8
import pandas as pd
import sys
import getopt
from pathlib import Path
import extract
import logging
import myLog
myLog.setup_logging()
logger = logging.getLogger(__name__)

pd.set_option("display.max_rows", None, "display.max_columns", None)
pd.set_option('expand_frame_repr', False)
try:
    opts, _ = getopt.getopt(sys.argv[1:],'uajle',['commune=', 'etat_administratif=', 'categrorie_juridique=', 'activite_unite=', 'activite_etablissement='])
except getopt.GetoptError :
        usage()
        sys.exit(2)
codeCommuneEtablissement = "*"
etatAdministratifUniteLegale = "A"
categorieJuridiqueUniteLegale = "5*"
activitePrincipaleUniteLegale = "*"
activitePrincipaleEtablissement="*"                                                              
for opt, arg in opts:

        if opt in ('-u', '--commune'):
            codeCommuneEtablissement=arg
        if opt in ('-a', '--etat_administratif'):
            etatAdministratifUniteLegale=arg
        if opt in ('-j', '--categrorie_juridique'):
            categorieJuridiqueUniteLegale=arg
        if opt in ('-l', '--activite_unite'):
            activitePrincipaleUniteLegale=arg
        if opt in ('-e', '--activite_etablissement'):
            #logger.debug("arg e ", arg)
            activitePrincipaleEtablissement=arg


pd.set_option("display.max_rows", None, "display.max_columns", None)


logger.debug("codeCommuneEtablissement : %s \n etatAdministratifUniteLegale : %s \n categorieJuridiqueUniteLegale : %s \n activitePrincipaleUniteLegale : %s \n activitePrincipaleEtablissement : %s \n ",codeCommuneEtablissement,  etatAdministratifUniteLegale,categorieJuridiqueUniteLegale,activitePrincipaleUniteLegale,activitePrincipaleEtablissement)

params = {"codeCommuneEtablissement": codeCommuneEtablissement,
    "etatAdministratifUniteLegale": etatAdministratifUniteLegale,
    'categorieJuridiqueUniteLegale': categorieJuridiqueUniteLegale,
    'activitePrincipaleUniteLegale': activitePrincipaleUniteLegale,'activitePrincipaleEtablissement':activitePrincipaleEtablissement}


fileName = extract.fileName(params)
fileDataName=fileName+".data"
if not Path(fileDataName).is_file():
    logger.error("Fichier non trouvé : %s", fileDataName)
    sys.exit(2)
    #file = extract.query(codeCommuneEtablissement=codeCommuneEtablissement, etatAdministratifUniteLegale=etatAdministratifUniteLegale, categorieJuridiqueUniteLegale=categorieJuridiqueUniteLegale, activitePrincipaleUniteLegale=activitePrincipaleUniteLegale, activitePrincipaleEtablissement=activitePrincipaleEtablissement, force=force)
    
    #myGoogleSearch.go(file)
    #fileDataName = builddata.build(codeCommuneEtablissement=codeCommuneEtablissement, etatAdministratifUniteLegale=etatAdministratifUniteLegale, categorieJuridiqueUniteLegale=categorieJuridiqueUniteLegale, activitePrincipaleUniteLegale=activitePrincipaleUniteLegale, activitePrincipaleEtablissement=activitePrincipaleEtablissement)
    
ts = pd.read_json(fileDataName, orient='index')

import natsort as ns

ts['groupEffectifUnite'] = pd.Categorical(ts['groupEffectifUnite'], ordered=True, categories= ns.natsorted(ts['groupEffectifUnite'].unique()))
ts = ts.sort_values('groupEffectifUnite')
byRegion=pd.pivot_table(ts,
    index=['region'],
    aggfunc='count',
    values=['siret'],
    margins=True)

print(byRegion)




byEffectif=pd.pivot_table(ts,index=['groupEffectifUnite' ],
                                        aggfunc='count',
                                        values=['siret'],
                                        margins=True)

print(byEffectif)

#print(ts[["siren", "siret", "CA 1", "CA 2", "CA 3"]])
bin_labels = ['Inconnu', '<100 k€', '100-500 k€', '500-1000 k€', '1-5 M€', '5-10  M€', '10-50 M€', ">50 M€"]
ts['groupCA'] = pd.cut(ts['CAAverage'].fillna(-88), [-100, 0, 100000, 500000, 1000000, 5000000, 10000000, 50000000, 900000000], include_lowest=True, labels=bin_labels)

pvtdf = ts.pivot_table(index='groupEffectifUnite', 
columns=['groupCA'], 
values='siren', 
aggfunc=('count'),
 margins=True).fillna(0)

print(pvtdf)
#print(ts["CAAverage"].isna())
pvtdf = ts.pivot_table(index='groupEffectifUnite', columns=['groupCA'], values='siren', aggfunc=('count'),margins=True)
print(pvtdf)
tsg = ts.groupby('groupEffectifUnite').agg({'CAAverage': 'mean', "siren": "count"})
print(tsg)
print (tsg.columns)

#print(ts["CAAverage"].isna().sum())


#print(ts)



pvdomain=pd.pivot_table(ts,index=['hasOwnedDomainFinal' ],
                                    columns=['groupEffectifUnite'],
                                    aggfunc='count',
                                    values=['siret'],
                                    margins=True)
print(pvdomain)
ts['CAAverageF'] = ts['CAAverage'].apply(lambda x: "{:,.0f} M€".format((x/1000000)))
#print(ts[ts['CAAverage']>5000000][["siren", "commune","activiteUnite", "activiteEtablissement", "denomination","groupEffectifUnite","groupEffectifEtablissement" ,"CAAverageF",'ownedDomainFinal']])

print(ts['effectifEtablissement'].sum())



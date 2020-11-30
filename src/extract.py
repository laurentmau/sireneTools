
import logging
from IPython.display import display 
import requests
import json
import sys
from api_insee import ApiInsee
from api_insee.criteria import Field
import secret 
import pandas as pd
import hashlib
import os.path
import configparser
from pathlib import Path
import config
pathDataFiles=config.paths["data"]
REGIONS = {
    'Auvergne-Rhône-Alpes': ['01', '03', '07', '15', '26', '38', '42', '43', '63', '69', '73', '74'],
    'Bourgogne-Franche-Comté': ['21', '25', '39', '58', '70', '71', '89', '90'],
    'Bretagne': ['35', '22', '56', '29'],
    'Centre-Val de Loire': ['18', '28', '36', '37', '41', '45'],
    'Corse': ['2A', '2B'],
    'Grand Est': ['08', '10', '51', '52', '54', '55', '57', '67', '68', '88'],
    'Guadeloupe': ['971'],
    'Guyane': ['973'],
    'Hauts-de-France': ['02', '59', '60', '62', '80'],
    'Île-de-France': ['75', '77', '78', '91', '92', '93', '94', '95'],
    'La Réunion': ['974'],
    'Martinique': ['972'],
    'Normandie': ['14', '27', '50', '61', '76'],
    'Nouvelle-Aquitaine': ['16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87'],
    'Occitanie': ['09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81', '82'],
    'Pays de la Loire': ['44', '49', '53', '72', '85'],
    'Provence-Alpes-Côte d\'Azur': ['04', '05', '06', '13', '83', '84'],
    'Outre-mer': ['97'],
}
EFFECTIFS = {
   '00':['0 salarié',0],
'01':['1 ou 2 salariés',1.5],
'02':['3 à 5 salariés',4],
'03':['6 à 9 salariés',7.5],
'11':['10 à 19 salariés',15],
'12':['20 à 49 salariés',35],
'21':['50 à 99 salariés',75],
'22':['100 à 199 salariés',200],
'31':['200 à 249 salariés',225],
'32':['250 à 499 salariés',375],
'41':['500 à 999 salariés',750],
'42':['1 000 à 1 999 salariés',1500],
'51':['2 000 à 4 999 salariés',3500],
'52':['5 000 à 9 999 salariés',7500],
'53':['10 000 salariés et plus',15000]
}
def effectif(row):
    for key, value in EFFECTIFS.items():
      if row['groupEffectif']== key:
         return value[1]
    return 0

def region(row):
    for key, value in REGIONS.items():
      if str(row['departement']) in  value:
         return key
    return "NO REGION"
def effectif(row):
    for key, value in EFFECTIFS.items():
      if row['groupEffectif']== key:
         return value[0]
    return 0

      
def query(logger, params,force=False):

    
    qe = ["%-30s : %s" % (k, v) for k, v in params.items()] 
    qe = "\n".join(qe)
    chksum = hashlib.md5(str(params).encode('utf-8')).hexdigest()
    logger.info("\n%-30s :\n%s", "QUERY",qe)
    
    fileDataName = pathDataFiles+"/"+str(chksum) + ".data"
    fileConfName = pathDataFiles+"/"+str(chksum) + ".conf"
    fileData = Path(fileDataName)
    if not force and fileData.is_file() :
        logger.info("Available (%s)",fileDataName)
        result= pd.read_json(fileDataName, orient='index')
    else:
        if force :
            logger.info("Force --> API call")
        else:
            logger.info("File not available --> API call")    
        api = ApiInsee(
            key = secret.sirene['key'],
            secret = secret.sirene['secret']
        )
        tot = 0
        data = api.siret(q=params)
        final=data.get()["header"]["total"]
        tot=0
        etablissements=[]
        logging.info("... total : "+str(final))
        for (page_index, page_result) in enumerate(data.pages(nombre=1000)):
            et=page_result['etablissements']
            tot=tot+len(et)
            logging.info("... +"+str(len(et))+" = "+str(tot)+"/"+str(final))
            if len(et) >0 :
                etablissements.append(pd.json_normalize(et))
        tous=pd.concat(etablissements)
        tous['activite'] = tous['uniteLegale.activitePrincipaleUniteLegale'].str[:3]
        tous['NAF 732']= tous['uniteLegale.activitePrincipaleUniteLegale'].replace( '[\.,)]','', regex=True )
        tous['departement'] = tous['adresseEtablissement.codeCommuneEtablissement'].str[:2]
        tous.rename(columns={'uniteLegale.activitePrincipaleUniteLegale':'activiteLong',
                                'uniteLegale.categorieEntreprise':'categorie',
                                'trancheEffectifsEtablissement':'effectif',
                                'adresseEtablissement.codeCommuneEtablissement':'commune',
                                'uniteLegale.categorieJuridiqueUniteLegale':'catJuridique',
                                'uniteLegale.denominationUniteLegale':"denomination", 
                             'uniteLegale.etatAdministratifUniteLegale': 'etatAdministratif'
                            }, inplace=True)
        tous = tous.astype({"siren":  str,"siret":  str})

        tous.reset_index(inplace=True)
        
        nafs=pd.read_csv(pathDataFiles+"/nafs.csv",sep=";")
        ts_naf=pd.merge (tous,nafs)

        finance=pd.read_csv(pathDataFiles+"/chiffres-cles-2019.csv",sep=";")
        finance.rename(columns={'Siren':'siren'
                                        }, inplace=True)
        finance = finance.astype({"siren":  str})
        ts_naf_fi = pd.merge(ts_naf, finance[['siren', 'CA 1', 'CA 2', 'CA 3']], on='siren', how='left')
        ts_naf_fi.reset_index(drop=True, inplace=True)

        ts_naf_fi['CA_average'] = ts_naf_fi[['CA 1', 'CA 2', 'CA 3']].mean(axis=1)
        ts_naf_fi.reset_index(drop=True, inplace=True)

        ts_naf_fi['CA_average'] = ts_naf_fi['CA_average'] / 1000
        ts_naf_fi['groupCA'] = pd.cut(ts_naf_fi['CA_average'].fillna(0), [0, 100, 500, 1000, 2000, 5000, 10000, 20000, 30000, 900000], include_lowest=True)
        ts_naf_fi['groupEffectif']=ts_naf_fi['effectif']
        ts_naf_fi['effectif'] = ts_naf_fi.apply(effectif, axis=1)
        ts_naf_fi['region'] = ts_naf_fi.apply(region, axis=1)
        ts_naf_fi.reset_index(drop=True, inplace=True)

        tousShort = ts_naf_fi[['siren', 'siret','activite','activiteLong','NAF 732','INTITULÉS 732',
                        'categorie','effectif','groupEffectif', 'effectifBrut',
                        'commune', 'region'
                        'catJuridique', 'denomination','departement','CA 1','CA 2', 'CA 3' , 'CA_average', 'groupCA','etatAdministratif']]
        tousShort.to_json(fileDataName, orient='index')
        config = configparser.ConfigParser()
        config['PARAMS'] = params
        with open(fileConfName, 'w') as configfile:
            config.write(configfile)
            
        result = tousShort
        
    
    logging.info("- - - - - - -\n%-30s: %s \n%-30s: %s","Nb entreprises",len(result.index), "Fichier", fileDataName)

    return fileDataName

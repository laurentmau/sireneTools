# coding: utf8
import logging
logger = logging.getLogger(__name__)
import myInit
import requests
import json
import sys
import api_insee
from api_insee import ApiInsee
from api_insee.criteria import Field, Periodic
import mySecrets
import pandas as pd
import hashlib
import os.path
import configparser
from pathlib import Path
secrets = mySecrets.getSecrets()

config = myInit.getConfig()
pathDataFiles = config["paths"]["data"]
REGIONS = {
    'Auvergne-Rhône-Alpes':
    ['01', '03', '07', '15', '26', '38', '42', '43', '63', '69', '73', '74'],
    'Bourgogne-Franche-Comté':
    ['21', '25', '39', '58', '70', '71', '89', '90'],
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
    'Nouvelle-Aquitaine':
    ['16', '17', '19', '23', '24', '33', '40', '47', '64', '79', '86', '87'],
    'Occitanie': [
        '09', '11', '12', '30', '31', '32', '34', '46', '48', '65', '66', '81',
        '82'
    ],
    'Pays de la Loire': ['44', '49', '53', '72', '85'],
    'Provence-Alpes-Côte d\'Azur': ['04', '05', '06', '13', '83', '84'],
    'Outre-mer': ['97'],
}
EFFECTIFS = {
    0: ['0 salarié', 0],
    1: ['1 ou 2 salariés', 1.5],
    2: ['3 à 5 salariés', 4],
    3: ['6 à 9 salariés', 7.5],
    11: ['10 à 19 salariés', 15],
    12: ['20 à 49 salariés', 35],
    21: ['50 à 99 salariés', 75],
    22: ['100 à 199 salariés', 200],
    31: ['200 à 249 salariés', 225],
    32: ['250 à 499 salariés', 375],
    41: ['500 à 999 salariés', 750],
    42: ['1 000 à 1 999 salariés', 1500],
    51: ['2 000 à 4 999 salariés', 3500],
    52: ['5 000 à 9 999 salariés', 7500],
    53: ['10 000 salariés et plus', 15000]
}


def calcEffectifs(df):
    for index, row in df.iterrows():
        df.at[index, 'effectifEtablissement'] = effectif(
            row['effectifEtablissementBrut'])
        df.at[index, 'groupEffectifEtablissement'] = groupEffectif(
            row['effectifEtablissementBrut'])
        df.at[index, 'effectifUnite'] = effectif(row['effectifUniteBrut'])
        df.at[index,
              'groupEffectifUnite'] = groupEffectif(row['effectifUniteBrut'])
    return df


def effectif(ef):
    if ef == "NN":
        return 0
    if not ef:
        return 0
    for key, value in EFFECTIFS.items():
        if int(ef) == key:
            return value[1]
    return 0


def groupEffectifToValeurEffectif(ef):
    if not ef:
        return 0
    for key, value in EFFECTIFS.items():
        if ef == value[0]:
            return value[1]
    return 0


def region(row):
    for key, value in REGIONS.items():
        if str(row['departement']) in value:
            return key
    return "NO REGION"


def groupEffectif(ef):
    """Clean "groupe effectif" column by replacing NAN values by "0 salarié"

    Args:
        ef (string): groupEffectif 

    Returns:
        string: cleaned groupEffectif
    """
    if ef == "NN":
        return "0 salarié"
    if not ef:
        return "0 salarié"
    for key, value in EFFECTIFS.items():
        if int(ef) == key:
            return value[0]
    return "0 salarié"


def fileName(params, pattern):
    """[summary]

    Args:
        params ([type]): [description]
        

    Returns:
        [type]: [description]
    """
    """Generate unique filename from a long string

    Args:
        params (str): string to be hashed
        pattern (str): pattern of the file

    Returns:
        str: the generated filename
    """
    pat = config["patterns"][pattern]

    chksum = hashlib.md5(str(params).encode('utf-8')).hexdigest()
    fileName = pathDataFiles + "/" + str(chksum) + "." + pat
    return fileName


def query(codeCommuneEtablissement="*",
          etatAdministratifUniteLegale="A",
          categorieJuridiqueUniteLegale="5*",
          activitePrincipaleUniteLegale="*",
          activitePrincipaleEtablissement="*",
          force=False):
    logger.debug("BEGIN")

    query = (
        Field('codeCommuneEtablissement', codeCommuneEtablissement)
        & Field('etatAdministratifUniteLegale', etatAdministratifUniteLegale)
        & Field('categorieJuridiqueUniteLegale', categorieJuridiqueUniteLegale)
        & Field('activitePrincipaleUniteLegale', activitePrincipaleUniteLegale)
        & Periodic(
            Field("activitePrincipaleEtablissement",
                  activitePrincipaleEtablissement)))

    params = {
        "codeCommuneEtablissement": codeCommuneEtablissement,
        "etatAdministratifUniteLegale": etatAdministratifUniteLegale,
        'categorieJuridiqueUniteLegale': categorieJuridiqueUniteLegale,
        'activitePrincipaleUniteLegale': activitePrincipaleUniteLegale,
        'activitePrincipaleEtablissement': activitePrincipaleEtablissement
    }

    logger.info(params)
    fileDataName = fileName(params, "eco")
    fileConfName = fileName(params, "conf")

    fileData = Path(fileDataName)
    if not force and fileData.is_file():
        logger.info("Available (%s)", fileDataName)
        result = pd.read_json(fileDataName, orient='index')
    else:
        if force:
            logger.info("Force --> API call")
        else:
            logger.info("File not available --> API call")
        api = ApiInsee(key=secrets['sirene']['key'],
                       secret=secrets['sirene']['secret'])
        tot = 0
        from datetime import date

        today = date.today()
        d1 = today.strftime("%Y-%m-%d")
        logger.debug("d1 : %s", d1)
        data = api.siret(q=query, date=d1)

        final = data.get()["header"]["total"]
        tot = 0
        etablissements = []
        logger.info("... total : " + str(final))
        for (page_index, page_result) in enumerate(data.pages(nombre=1000)):
            et = page_result['etablissements']
            tot = tot + len(et)
            logger.info("... +" + str(len(et)) + " = " + str(tot) + "/" +
                        str(final))
            if len(et) > 0:
                etablissements.append(pd.json_normalize(et))
        tous = pd.concat(etablissements)
        tous.loc[:, 'activiteEtablissement'] = tous.periodesEtablissement.map(
            lambda x: x[0]['activitePrincipaleEtablissement'])
        tous['departement'] = tous[
            'adresseEtablissement.codeCommuneEtablissement'].str[:2]

        tous.rename(columns={
            'uniteLegale.activitePrincipaleUniteLegale':
            'activiteUnite',
            'uniteLegale.categorieEntreprise':
            'categorie',
            'trancheEffectifsEtablissement':
            'effectifEtablissementBrut',
            'uniteLegale.trancheEffectifsUniteLegale':
            'effectifUniteBrut',
            'adresseEtablissement.codeCommuneEtablissement':
            'commune',
            'uniteLegale.categorieJuridiqueUniteLegale':
            'catJuridique',
            'uniteLegale.denominationUniteLegale':
            "denomination",
            'uniteLegale.etatAdministratifUniteLegale':
            'etatAdministratif'
        },
                    inplace=True)
        tous = tous.astype({"siren": int, "siret": int})

        tous.reset_index(drop=True, inplace=True)
        logging.debug("before nafs loading")

        nafs = pd.read_csv(config["nafs"], sep=",")
        logging.debug("after nafs loading")

        logging.debug(nafs.columns)
        tous = pd.merge(tous, nafs, left_on="activiteUnite", right_on="CODE")
        tous.rename(columns={"LIBELLE": "activiteUniteLibelle"}, inplace=True)
        tous = tous.merge(nafs,
                          left_on="activiteEtablissement",
                          right_on="CODE")
        tous.rename(columns={"LIBELLE": "activiteEtablissementLibelle"},
                    inplace=True)
        tous.reset_index(inplace=True)
        chunksize = 300000

        #for chunk in  pd.read_csv(pathDataFiles+"/chiffres-cles-2019.csv", sep=';', chunksize=chunksize):
        chunk = pd.read_csv(pathDataFiles + "/chiffres-cles-2019-light.csv",
                            sep=';')
        logger.debug("Chunk")
        chunk.rename(columns={'Siren': 'siren'}, inplace=True)
        logger.debug("after rename")

        #chunk = chunk.astype({"siren": str})
        #logger.debug(chunk[['siren', 'CA 1', 'CA 2', 'CA 3']])
        tous = pd.merge(tous, chunk, how="left")
        #ts_naf_fi.reset_index(drop=True, inplace=True)
        #ts_naf_fi = pd.merge(ts_naf_fi, chunk[['siren', 'CA 1', 'CA 2', 'CA 3', 'tranche_ca_millesime_1', 'tranche_ca_millesime_2', 'tranche_ca_millesime_3']])
        logger.debug("after merge")

        result = tous[[
            'siren', 'siret', 'activiteUnite', 'activiteUniteLibelle',
            'activiteEtablissement', 'activiteEtablissementLibelle',
            'categorie', 'effectifEtablissementBrut', 'effectifUniteBrut',
            'commune', 'catJuridique', 'denomination', 'departement', 'CA 1',
            'CA 2', 'CA 3', 'etatAdministratif'
        ]]
        result.reset_index(drop=True, inplace=True)
        result.to_json(fileDataName, orient='index')
        result = pd.read_json(fileDataName, orient='index')

    result['CAAverage'] = result[['CA 1', 'CA 2', 'CA 3'
                                  ]].mean(axis=1) / 1000000

    result = calcEffectifs(result)
    result["CAAverageCorrected"] = result['CAAverage'] * result[
        'effectifEtablissement'] / result['effectifUnite']
    bin_labels = [
        'Inconnu', '<100 k€', '100-500 k€', '500-1000 k€', '1-5 M€',
        '5-10  M€', '10-50 M€', ">50 M€"
    ]
    result['groupCA'] = pd.cut(result['CAAverageCorrected'].fillna(-88),
                               [-100, 0, 0.1, 0.5, 1, 5, 10, 50, 900],
                               include_lowest=True,
                               labels=bin_labels)
    result.reset_index(drop=True, inplace=True)

    #result['groupEffectifEtablissement']=result.apply(groupEffectif, axis=1)
    #result['effectif'] = result.apply(effectif, axis=1)
    result['region'] = result.apply(region, axis=1)

    indexNames = result[result['effectifEtablissement'] == 0].index
    result.drop(indexNames, inplace=True)
    result.reset_index(drop=True, inplace=True)
    import natsort as ns
    result['groupEffectifEtablissement'] = pd.Categorical(
        result['groupEffectifEtablissement'],
        ordered=True,
        categories=ns.natsorted(result['groupEffectifEtablissement'].unique()))
    result = result.sort_values('groupEffectifEtablissement')

    result.to_json(fileDataName, orient='index')
    queryConfig = configparser.ConfigParser()
    queryConfig['PARAMS'] = params
    with open(fileConfName, 'w') as configfile:
        queryConfig.write(configfile)

    logging.info("%-30s: %s", "Nb entreprises", len(result.index))
    logging.info("%-30s: %s", "Fichier", fileDataName)

    return fileDataName

# coding: utf8

import logging
logger = logging.getLogger(__name__)
import myInit
import traceback
from urllib.parse import urlparse

config = myInit.getConfig()

import pandas as pd
import numpy as np
import time
import tldextract
logger_tdl = logging.getLogger("tldextract")
logger_tdl.setLevel(logging.WARNING)

import extract
from pathlib import Path
import myInit
pathDataFiles = config["paths"]["data"]
from difflib import SequenceMatcher
import textdistance
from googlesearch import search
import requests
from requests_html import HTMLSession


def mySearch(row):
    logger.debug("begin mySearch")
    if 'NOTYET' in row["search"]:
        time.sleep(1)
        try:
            s = []
            for u in search(row['denomination'],
                            tld='fr',
                            lang='fr',
                            num=5,
                            stop=5,
                            pause=2.0):
                logger.debug(u)
                s.append(u)
            return s
        except:
            logger.debug("error")
            traceback.print_exc()
            return "NOTYET"
    else:
        logger.debug("return row search")
        return row['search']


def searchLinks(row):
    if True:
        time.sleep(1)
        try:
            s = []
            for u in search(row['denomination'],
                            tld='fr',
                            lang='fr',
                            num=5,
                            stop=5,
                            pause=2.0):
                s.append(u)
            return s
        except:
            traceback.print_exc()
            return "NOTYET"
    else:
        return row['search']


def searchGoogle(df, fileDataName):
    logger.info("BEGIN OF searchGoogle")
    if "search" not in df.columns:
        df["search"] = "NOTYET"
    bufferSearchName = config["buffers"]["search"]

    bufferFile = Path(bufferSearchName)
    if bufferFile.is_file():
        bufferSearch = pd.read_pickle(bufferFile, compression='gzip')
    else:
        bufferSearch = pd.DataFrame(columns=['denomination', 'search'])
        bufferSearch = bufferSearch.append(
            {
                'denomination': "first",
                'search': "first"
            }, ignore_index=True)

    df['isSearched'] = df.apply(isSearched, axis=1)
    dfSize = len(df.index)
    nbToSearch = len(df[df['isSearched'] == False])
    logger.info(" %d / %d (to be searched / total)", nbToSearch, dfSize)

    for index, row in df[df['isSearched'] == False].iterrows():
        buf = bufferSearch.loc[bufferSearch['denomination'] ==
                               row['denomination'], 'search']
        if len(buf) == 0:
            r = searchLinks(row)
            logger.debug("Not in buffer %s : %s", row['denomination'], r)
            bufferSearch = bufferSearch.append(
                {
                    'denomination': row['denomination'],
                    "search": r
                },
                ignore_index=True)

        else:
            r = buf.iloc[0]
            logger.debug("In buffer %s, %s", row['denomination'], r)

        df.at[index, 'search'] = r
        if index % 100 == 0:
            logger.info("(%d / %d) (file saved)", index + 1, nbToSearch)
            bufferSearch.to_pickle(bufferSearchName, compression='gzip')
    logger.info("(%d / %d) (file saved)", nbToSearch, nbToSearch)
    bufferSearch.to_pickle(bufferSearchName, compression='gzip')
    logger.info("END OF searchGoogle")

    return df


def searchTitle(df, fileDataName):
    logger.info("BEGIN OF searchTitle")
    if "titleDomain" not in df.columns:
        df["titleDomain"] = "NOTYET"
    bufferName = config["buffers"]["titleDomain"]

    bufferFile = Path(bufferName)
    if bufferFile.is_file():
        bufferSearch = pd.read_pickle(bufferFile, compression='gzip')
    else:
        bufferSearch = pd.DataFrame(columns=['domain', 'titleDomain'])
        bufferSearch = bufferSearch.append(
            {
                'domain': "first",
                "titleDomain": "first"
            }, ignore_index=True)

    dfSize = len(df.index)
    nbToSearch = len(df[df['titleDomain'] == "NOTYET"])
    logger.info(" %d / %d (to be searched / total)", nbToSearch, dfSize)

    for index, row in df[df['titleDomain'] == "NOTYET"].iterrows():
        titles = []
        for d in row["domains"]:
            buf = bufferSearch.loc[bufferSearch['domain'] == d, 'titleDomain']
            if len(buf) == 0:

                #logger.debug(
                #    "No title for %s -->let's search - - - - - - - - - - -", d)
                try:
                    session = HTMLSession()
                    response = session.get("http://" + d, timeout=10)
                    t = response.html.find('title', first=True).text
                except:
                    t = "NOTFOUND"
                # if t == "NOTFOUND":
                #     logger.debug("KO : %s", d)
                # else:
                #     logger.debug("OK : %s : %s", d, t)
                bufferSearch = bufferSearch.append(
                    {
                        'domain': d,
                        "titleDomain": t
                    }, ignore_index=True)
                titles.append(t)

            else:
                #logger.debug("In buffer %s, %s", d, buf.iloc[0])
                titles.append(buf.iloc[0])

        logger.debug("update of titleDomain for %s with %s",
                     row["denomination"], titles)
        df.at[index, 'titleDomain'] = titles

        if index % 100 == 0:
            logger.info("(%d / %d) (file saved)", index + 1, nbToSearch)
            df.to_json(fileDataName, orient='index')
            bufferSearch.to_pickle(bufferName, compression='gzip')
    logger.info("(%d / %d) (file saved)", nbToSearch, nbToSearch)
    bufferSearch.to_pickle(bufferName, compression='gzip')
    for index, row in df.iterrows():
        df.at[index, 'ownedDomain3'] = ownedDomain3('domain', row)
        df.at[index, 'scoreOwnedDomain3'] = ownedDomain3('score', row)
    logger.info("END OF searchTitle")

    return df


def ownedDomainFinal(row):
    d = [row['ownedDomain1'], row['ownedDomain2'], row['ownedDomain3']]
    dnonone = list(filter(lambda a: a != "NONE", d))
    nb = len(list(dict.fromkeys(d)))
    nbnonone = len(list(dict.fromkeys(dnonone)))
    if nbnonone == 0:
        return "NONE"
    if row['ownedDomain1'] != "NONE":
        return row['ownedDomain1']
    best = row['ownedDomain2']
    bestScore = row['scoreOwnedDomain2']

    if row['scoreOwnedDomain3'] > bestScore:
        best = row['ownedDomain3']
        bestScore = row['scoreOwnedDomain3']
    return best


def confidenceOwnedDomainFinal(row):
    if row['ownedDomainFinal'] == "NONE":
        return -1
    if row['ownedDomain1'] != "NONE":
        if row['ownedDomain1'] == row['ownedDomain2'] and row[
                'ownedDomain3'] == row['ownedDomain1']:
            return 100  # 1=2=3

        if row['ownedDomain1'] != row['ownedDomain2'] and row[
                'ownedDomain3'] == row['ownedDomain2']:
            return 90  #1 from Google but !=2  and 2=3
        if row['ownedDomain1'] != row['ownedDomain2'] and row[
                'ownedDomain1'] != row['ownedDomain3'] and row[
                    'ownedDomain2'] != row['ownedDomain3']:
            return 95  #1 from Google and 1!=2!=3
        if (row['ownedDomain1'] != row['ownedDomain2']
                and row['ownedDomain1'] == row['ownedDomain3']) or (
                    row['ownedDomain1'] == row['ownedDomain2']
                    and row['ownedDomain1'] != row['ownedDomain3']):
            return 99  #1 from google and 1= 2 or 1=3
    else:
        if row['ownedDomain2'] == row['ownedDomain3']:
            return 10  # nothing from google, and  2=3
        else:
            return 5  # nothing from google, and  2!=3

    return -100  #error


def domains(row):
    ld = []
    logger.debug("%s : %s", row["denomination"], row["search"])
    for item in row["search"]:
        d = tldextract.extract(item)
        dom = '.'.join(d[1:3])
        logger.debug("item : %s d : %s domain : %s", item, d, dom)
        ld.append(dom)
    return ld


def firstDomain(row):
    logging.debug("domains %s", row["domains"])
    if row["domains"]:
        return row["domains"][0]
    else:
        return "NONE"


def ownedDomain2(row, domainsToRemove=[]):
    rmax = 0
    r = 0
    owned = "NONE"
    for item in row["domains"]:
        if item not in domainsToRemove:
            r = textdistance.ratcliff_obershelp(item.upper(),
                                                row['denomination'].upper())
        else:
            r = 0
        if r > rmax:
            rmax = r
            owned = item
    return owned


def ownedDomain3(type, row):
    rmax = 0
    r = 0
    owned = "NONE"
    logger.debug("%s, %s, %s", row['denomination'].upper(), row["domains"],
                 row["titleDomain"])

    for domain, title in zip(row["domains"], row["titleDomain"]):
        if title and title != "NOTFOUND":
            r = textdistance.ratcliff_obershelp(title.upper(),
                                                row['denomination'].upper())
            logger.debug("%s vs %s : %f", title.upper(),
                         row['denomination'].upper(), r)
            if r >= rmax:
                rmax = r
                owned = domain

    if type == "domain":
        return owned
    else:
        return rmax


def cleanDomains(df):
    ld = []
    sirens = []
    domainsToRemove = []
    bufferName = config["buffers"]["domainsToRemove"]

    bufferFile = Path(bufferName)
    if bufferFile.is_file():
        bufferDomainsToRemove = pd.read_pickle(bufferFile, compression='gzip')
    else:
        bufferDomainsToRemove = pd.DataFrame(columns=['domain'])
        bufferDomainsToRemove = bufferDomainsToRemove.append(
            {'domain': "first"}, ignore_index=True)

    for index, row in df.iterrows():
        if row["siren"] not in sirens:
            for dom in row["domains"]:
                ld.append(dom)
        sirens.append(row["siren"])

    for d in ld:
        if ld.count(d) > 2:
            #logger.debug(".")
            domainsToRemove.append(d)
    #logger.debug(".")
    bufferDomainsToRemoveList = bufferDomainsToRemove['domain'].tolist()
    domainsToRemove = list(
        dict.fromkeys(domainsToRemove + bufferDomainsToRemoveList))
    logger.debug("Domains to remove %s", domainsToRemove)
    df['domains'] = [[item for item in l if item not in set(domainsToRemove)]
                     for l in df['domains']]
    df['ownedDomain1'] = df['firstDomain'].apply(
        lambda x: x if x not in set(domainsToRemove) else "NONE")

    bufferDomainsToRemove = pd.DataFrame(domainsToRemove, columns=['domain'])
    bufferDomainsToRemove.to_pickle(bufferName, compression='gzip')
    return df


def scoreOwnedDomain2(row):
    if row['ownedDomain2'] == "NONE":
        return 0
    else:
        return textdistance.ratcliff_obershelp(row['ownedDomain2'].upper(),
                                               row['denomination'].upper())


def scoreOwnedDomain1(row):
    if row['ownedDomain1'] == "NONE":
        return 0
    else:
        return textdistance.ratcliff_obershelp(row['ownedDomain1'].upper(),
                                               row['denomination'].upper())


def isFacebook(row):
    for item in row["search"]:
        if "facebook.com" in item:
            return item
    return "NO"


def isLinkedin(row):
    for item in row["search"]:
        if "linkedin" in item:
            return item
    return "NO"


def isSearched(row):
    if 'NOTYET' in row["search"]:
        return False
    return True


def nbSearch(row):
    return len(row["search"])


def go(codeCommuneEtablissement="*",
       etatAdministratifUniteLegale="A",
       categorieJuridiqueUniteLegale="5*",
       activitePrincipaleUniteLegale="*",
       activitePrincipaleEtablissement="*",
       force=False):
    params = {
        "codeCommuneEtablissement": codeCommuneEtablissement,
        "etatAdministratifUniteLegale": etatAdministratifUniteLegale,
        'categorieJuridiqueUniteLegale': categorieJuridiqueUniteLegale,
        'activitePrincipaleUniteLegale': activitePrincipaleUniteLegale,
        'activitePrincipaleEtablissement': activitePrincipaleEtablissement
    }
    logger.info(params)
    fileDataName = extract.fileName(params, "web")
    fileData = Path(fileDataName)
    if not force and fileData.is_file():
        logger.info("Available (%s)", fileDataName)
        result = pd.read_json(fileDataName, orient='index')
        return result

    if force:
        logger.info("Force --> recalc")
    else:
        logger.info("File not available -->calc")

    fileEcoName = extract.fileName(params, "eco")
    fileEco = Path(fileEcoName)
    if not fileEco.is_file():
        logger.error("eco file not found (%s)", fileEcoName)
        sys.exit(2)
    else:
        ts = pd.read_json(fileEcoName, orient='index')
        ts = ts[["siret", "siren", "denomination"]]
        ts['search'] = "NOTYET"
        ts.to_json(fileDataName, orient='index')

    logger.debug(".")
    ts.reset_index(drop=True, inplace=True)

    ts = searchGoogle(ts, fileDataName)
    logger.debug(".")

    ts.to_json(fileDataName, orient='index')
    logger.debug(".")

    ts['domains'] = ts.apply(domains, axis=1)
    logger.debug(".")

    ts.reset_index(drop=True, inplace=True)
    logger.debug(".")
    ts['firstDomain'] = ts.apply(firstDomain, axis=1)

    ts = cleanDomains(ts)
    logger.debug(".")
    ts.reset_index(drop=True, inplace=True)

    ts = searchTitle(ts, fileDataName)
    logger.debug(".")
    ts['ownedDomain2'] = ts.apply(ownedDomain2, axis=1)
    logger.debug("columns : %s", ts.columns)

    ts['scoreOwnedDomain1'] = ts.apply(scoreOwnedDomain1, axis=1)
    ts['scoreOwnedDomain2'] = ts.apply(scoreOwnedDomain2, axis=1)
    logger.debug(".")

    ts.reset_index(drop=True, inplace=True)
    ts['ownedDomainFinal'] = ts.apply(ownedDomainFinal, axis=1)
    ts['confidenceOwnedDomainFinal'] = ts.apply(confidenceOwnedDomainFinal,
                                                axis=1)

    ts['hasOwnedDomainFinal'] = np.where(ts['ownedDomainFinal'] != 'NONE',
                                         True, False)
    logger.debug(".")

    ts['nbSearch'] = ts.apply(nbSearch, axis=1)
    ts['facebook'] = ts.apply(isFacebook, axis=1)
    ts['linkedIn'] = ts.apply(isLinkedin, axis=1)

    ts['isSearched'] = ts.apply(isSearched, axis=1)
    ts['isFacebook'] = ts.apply(isFacebook, axis=1)
    ts['isLinkedIn'] = ts.apply(isLinkedin, axis=1)

    ts.reset_index(drop=True, inplace=True)
    ts.to_json(fileDataName, orient='index')
    logger.info("Done.")

    return ts

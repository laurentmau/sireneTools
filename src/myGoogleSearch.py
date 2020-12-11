# coding: utf8

import logging
logger = logging.getLogger(__name__)

import pandas as pd
import numpy as np
import time
import tldextract

import extract
from pathlib import Path
import config
pathDataFiles=config.paths["data"]
from difflib import SequenceMatcher
import textdistance
from googlesearch import search
import requests
from requests_html import HTMLSession

def mySearch(row):
    if 'NOTYET' in row["search"]:
        time.sleep(1)
        try:
            s=search(row['denomination'])
            return s
        except:
            return "NOTYET"
    else:
        return row['search']
        
def searchGoogle(df, fileDataName):
    logger.debug("BEGIN")
    if "search" not in df.columns:
        df["search"] = "NOTYET"
    bufferSearchName=config.buffers["search"]
    
    bufferFile = Path(bufferSearchName)
    if  bufferFile.is_file() :
        bufferSearch = pd.read_pickle(bufferFile, compression='gzip')
        #bufferSearch = pd.read_csv(bufferFile)
    else:
        bufferSearch = pd.DataFrame(columns=['denomination', 'search'])
        bufferSearch=bufferSearch.append({'denomination':"first", 'search':"first"}, ignore_index=True)

    df['isSearched'] = df.apply(isSearched, axis=1)
    dfSize = len(df.index)
    nbToSearch = len(df[df['isSearched'] == False])
    logger.info(" %d / %d (to be searched / total)", nbToSearch, dfSize ) 
    
    for index, row in df[df['isSearched'] == False].iterrows():
        buf=bufferSearch.loc[bufferSearch['denomination'] == row['denomination'], 'search']
        if len(buf) == 0:
            logger.debug("Not in buffer %s",  row['denomination'])
            r=mySearch(row)
            bufferSearch=bufferSearch.append({'denomination':row['denomination'], "search":r}, ignore_index=True)

        else:
            r=buf.iloc[0]
            logger.debug("In buffer %s, %s",  row['denomination'],r)

        df.at[index,'search'] = r
        if index % 10 == 0:
            logger.info("%s (%d / %d) (file saved)", row['denomination'], index+1, nbToSearch)
            df.to_json(fileDataName, orient='index')
            bufferSearch.to_pickle(bufferSearchName, compression='gzip')
            #bufferSearch.to_csv(bufferSearchName, index=False)
    bufferSearch.to_pickle(bufferSearchName, compression='gzip')
    #bufferSearch.to_csv(bufferSearchName, index=False)
    return df

def searchTitle(df,fileDataName ):
    if "titleDomain" not in df.columns:
        df["titleDomain"] = "NOTYET"
    bufferName=config.buffers["titleDomain"]
    
    bufferFile = Path(bufferName)
    if  bufferFile.is_file() :
        bufferSearch = pd.read_pickle(bufferFile, compression='gzip')
        #bufferSearch = pd.read_csv(bufferFile)
    else:
        bufferSearch = pd.DataFrame(columns=['domain', 'titleDomain'])
        bufferSearch=bufferSearch.append({'domain':"first", "titleDomain":"first"}, ignore_index=True)

    dfSize = len(df.index)
    nbToSearch = len(df[df['titleDomain'] == "NOTYET"])
    logger.info(" %d / %d (to be searched / total)", nbToSearch, dfSize ) 
    
    for index, row in df[df['titleDomain'] == "NOTYET"].iterrows():
        titles = []
        for d in row["domains"]:
            buf=bufferSearch.loc[bufferSearch['domain'] == d, 'titleDomain']
            if len(buf) == 0:
                
                logger.debug("No title for %s -->let's search - - - - - - - - - - -", d)
                try:
                    session = HTMLSession()
                    response = session.get("http://"+d, timeout=10)
                    t = response.html.find('title', first=True).text
                except:
                    t="NOTFOUND"
                if t=="NOTFOUND":
                    logger.debug("KO : %s", d)
                else:
                    logger.debug("OK : %s : %s", d, t)
                    bufferSearch=bufferSearch.append({'domain':d, "titleDomain":t}, ignore_index=True)
                titles.append(t)
                
            else:
                logger.debug("In buffer %s, %s",  d,buf.iloc[0])
                titles.append(buf.iloc[0])
                
        logger.debug("update of titleDomain for %s with %s", row["denomination"], titles)
        df.at[index,'titleDomain'] = titles


        #logger.debug("index: %d", index)
        if index % 10 == 0:
            logger.info("(%d / %d) (file saved)", index+1, nbToSearch)
            df.to_json(fileDataName, orient='index')
            bufferSearch.to_pickle(bufferName, compression='gzip')
            #bufferSearch.to_csv(bufferName, index=False)
        bufferSearch.to_pickle(bufferName, compression='gzip')
        #bufferSearch.to_csv(bufferName,index=False)
    for index, row in df.iterrows():
        df.at[index, 'ownedDomain3']=ownedDomain3('domain', row)
        df.at[index, 'scoreOwnedDomain3']=ownedDomain3('score', row)

    return df

def statusDomain(row):
    d=[row['ownedDomain'], row['ownedDomain2'], row['ownedDomain3']]
    dnonone=list(filter(lambda a: a !="NONE", d))
    nb=len(list(dict.fromkeys(d)))
    nbnonone=len(list(dict.fromkeys(dnonone)))
    if nbnonone ==0:
        return "NONE"
    best=row['ownedDomain']
    bestScore=row['scoreOwnedDomain']
    if row['scoreOwnedDomain2'] > bestScore:
        best=row['ownedDomain2']
        bestScore=row['scoreOwnedDomain2']
    
    if row['scoreOwnedDomain3'] > bestScore:
        best=row['ownedDomain3']
        bestScore=row['scoreOwnedDomain3']
    return best

def domains(row):
    ld = []
    logger.debug("%s : %s",row["denomination"], row["search"] )    
    for item in row["search"]:
        d = tldextract.extract(item)
        dom = '.'.join(d[1:3])
        logger.debug("item : %s d : %s domain : %s",item, d, dom ) 
        ld.append(dom)
    return ld

def ownedDomain(row):
    domainsToRemove=['societe.com', 'dnb.com', 'bloomberg.com', 'verif.com', 'yelp.com', 'lefigaro.fr', 'expedia.com', 'facebook.com', 'linkedin.com', 'kompass.com', 'pagesjaunes.fr', 'le-guide-sante.org', '118712.fr', 'chefdentreprise.com' , 'procedurecollective.fr', 'societeinfo.com', 'mappy.com', 'booking.com', 'bfmtv.com' , 'politologue.com', 'linternaute.com', 'tripadvisor.com', 'manageo.fr', 'infogreffe.com', 'score3.fr', 'guide-forestier.com', 'europages.fr', 'bois-de-france.org', 'ellisphere.fr', 'ceginformacio.hu', 'annuaire-inverse-france.com', 'opencorporates.com', 'actulegales.fr', 'infogreffe.fr', 'fordaq.com', 'materiau.info', 'lesechos.fr', 'bilansgratuits.fr', 'apollo.io', 'eterritoire.fr', 'starofservice.com', 'annuairefrancais.fr', 'youtube.com', 'site-solocal.com', 'e-pro.fr', 'wikipedia.org', 'cylex-locale.fr', 'produitsbois-grandest.com', 'le-site-de.com', 'edecideur.info', 'woodexchangeassociation.com', 'bois-de-chauffage.net', 'tripadvisor.com', 'artisanatbourgogne.fr', 'linternaute.com', 'google.com', 'ets.org', 'poseur-parquet.com', 'repreneurs.com', 'guyane-bois.net', 'lemarchedubois.com','118000.fr', 'hoodspot.fr', 'annuaire-horaire.fr', 'laforetbouge.fr', 'gowork.fr', 'hello-industry.com', 'pple.fr', 'infobel.com', 'boisdulimousin.fr', 'fibois.com', 'suppliersof.com', 'usinenouvelle.com', 'pinsdefrance.com', 'twitter.com', 'profilwood.eu', 'aisne.cci.fr', 'vosgesmatin.fr', 'fnbois.com', 'euro-listings.com', 'meilleur-artisan.com', 'instagram.com', '500px.com', 'frenchtimber.com', 'telephone.city', 'vainu.io', 'bois-et-vous.fr', 'imdb.com', 'impayes.com', 'waze.com', 'pinterest.fr','grenoble.cci.fr', 'decidento.com', 'estrepublicain.fr', 'vymaps.com', 'wixsite.com', 'leprogres.fr', 'trouver-ouvert.fr', 'amazon.com', 'pitchbook.com', 'convention-entreprise.fr', 'annuaire-mairie.fr', 'pinterest.ca','pagespro.com', 'panjiva.com', 'francetvinfo.fr', 'zebottin.fr', 'pagesperso-orange.fr', 'journaldunet.com', 'laposte.fr ', 'republicain-lorrain.fr', 'salespider.com', 'yelp.ca', 'edecideur.com', 'airbnb.com ', 
    'french-corporate.com', 'maceuro.com', 'boislocalbretagne.bzh','boislocalbretagne.bzh', 'springer.com', 'jurabois.fr', 'laposte.fr', 'boislim.fr', 'toutle05.fr', 'importgenius.com',  'yelp.ca', 'lozere.cci.fr', 'sireninfo.com', 'neofor.com', 'airbnb.com', 'poujoulat.group', 'ouest-france.fr', 'pinterest.at', 'pinterest.com', 'lavieimmo.com', 'annonces-legales.fr', 'horaire24.com'
     ]
    rmax = 0
    r = 0
    owned="NONE"
    for item in row["domains"]:
        if item not in domainsToRemove:
            r = textdistance.ratcliff_obershelp(item.upper(), row['denomination'].upper())
            
        else:
            r = 0
        if r > rmax:
            rmax = r
            owned=item

    return owned

def ownedDomain2(row, domainsToRemove=[]):
    rmax = 0
    r = 0
    owned="NONE"
    for item in row["domains"]:
        if item not in domainsToRemove:
            r = textdistance.ratcliff_obershelp(item.upper(), row['denomination'].upper())
        else:
            r = 0
        if r > rmax:
            rmax = r
            owned=item
    return owned
def ownedDomain3(type, row):
    rmax = 0
    r = 0
    owned="NONE"
    logger.debug("%s, %s, %s",row['denomination'].upper(), row["domains"],row["titleDomain"]  ) 

    for domain, title in zip(row["domains"], row["titleDomain"]):
        if title :
            r = textdistance.ratcliff_obershelp(title.upper(), row['denomination'].upper())
            logger.debug("%s vs %s : %f",title.upper(),row['denomination'].upper(), r  ) 
            if r >= rmax:
                rmax = r
                owned = domain

    if type == "domain":
        return owned
    else:
        return rmax

def cleanDomains(df, fileDataName):
    ld=[]
    sirens=[]
    domainsToRemove = []
    #bug(".")

    for index, row in df.iterrows():
        if row["siren"] not in sirens:
            for dom in row["domains"]:
                #logger.debug("%d not sirens", row["siren"])
                ld.append(dom)
        sirens.append(row["siren"])
        #logger.debug(index)
    #logger.debug(".")
    # df=pd.DataFrame({'Number': List})
    # df1 = pd.DataFrame(data=df['Number'].value_counts(), columns=[['Number', 'Count']])
    # df1['Count']=df1['Number'].index 

    for d in ld:
        if ld.count(d) > 2:
            #logger.debug(".")
            domainsToRemove.append(d)
    #logger.debug(".")
    
    domainsToRemove = list(dict.fromkeys(domainsToRemove))
    logger.debug("Domains to remove %s", domainsToRemove)
    df['domains'] = [[item for item in l if item not in set(domainsToRemove)] for l in df['domains']]
    #df['domains'] = df['domains'].map(set(domainsToRemove).difference).map(list)
    return df


def calcOwnedDomains_hide(df, fileDataName ):

    logger.info("Begin calcOwnedDomains") 
    if "ownedDomain2" not in df.columns:
        df["ownedDomain2"] = "-"
    if "titleDomain" not in df.columns:
        df["titleDomain"] = "-"
    

    maxLoop = 100
    domainsToRemove=[]
    for loop in range(maxLoop):
        group = df[['ownedDomain2', 'denomination']].groupby(['ownedDomain2']).denomination.agg([len]).sort_values(ascending=False,by='len')
        dTR = group[group['len'] > 2].index.values
        if loop > 1 and len(dTR) == 1:
            for index, row in df.iterrows():
                if not row['ownedDomain2'] == "NONE":
                    r = textdistance.ratcliff_obershelp(row['ownedDomain2'].upper(), row['denomination'].upper())
                    if r < 0.25:
                        #logger.debug("On supprime %s %s (%r)", row['ownedDomain2'], row['denomination'].upper(), r )
                        df.at[index, 'ownedDomain2']="NONE"
            
            return df
        #logger.debug("Loop %d , %d domains to remove : %s", loop,len(dTR), dTR.tolist() )

        domainsToRemove = list( set(domainsToRemove + dTR.tolist()))

        for index, row in df.iterrows():
            df.at[index, 'ownedDomain2'] = ownedDomain2(row, domainsToRemove)

    return df

def scoreOwnedDomain2(row):
    if row['ownedDomain2'] == "NONE":
        return 0
    else:
        return textdistance.ratcliff_obershelp(row['ownedDomain2'].upper(), row['denomination'].upper())



def scoreOwnedDomain(row):
    if row['ownedDomain'] == "NONE":
        return 0
    else:
        return textdistance.ratcliff_obershelp(row['ownedDomain'].upper(), row['denomination'].upper())

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






def go(dumpFile):
    fileDataName=pathDataFiles+"/"+dumpFile
    fileData = Path(fileDataName)
    if  not fileData.is_file() :
        raise NameError("FIle not found (%s)",fileDataName)
    else:
        ts = pd.read_json(fileDataName, orient='index')
        if 'search' not in ts.columns:
            ts['search'] = "NOTYET"
    logger.debug(".")

    ts=searchGoogle(ts, dumpFile )
    logger.debug(".")

    ts.to_json(fileDataName, orient='index')
    logger.debug(".")

    ts['domains'] = ts.apply(domains, axis=1)
    logger.debug(".")

    ts.reset_index(drop=True, inplace=True)
    logger.debug(".")

    ts = cleanDomains(ts, dumpFile)
    logger.debug(".")
    
    ts=searchTitle(ts,dumpFile)
    logger.debug(".")
    ts['ownedDomain'] = ts.apply(ownedDomain, axis=1)
    ts['ownedDomain2'] =  ts.apply(ownedDomain2, axis=1)
    #ts=calcOwnedDomains(ts,logger,fileDataName)
    ts['scoreOwnedDomain'] = ts.apply(scoreOwnedDomain, axis=1)
    ts['scoreOwnedDomain2'] = ts.apply(scoreOwnedDomain2, axis=1)
    logger.debug(".")

    ts.reset_index(drop=True, inplace=True)
    ts['ownedDomainFinal'] = ts.apply(statusDomain, axis=1)
    ts['hasOwnedDomainFinal'] = np.where(ts['ownedDomainFinal']!= 'NONE', True, False)    
    logger.debug(".")

    ts['nbSearch'] = ts.apply(nbSearch, axis=1)
    ts['facebook']=ts.apply(isFacebook, axis=1)
    ts['linkedIn'] = ts.apply(isLinkedin, axis=1)

    ts['isSearched'] = ts.apply(isSearched, axis=1)
    ts['isFacebook']=ts.apply(isFacebook, axis=1)
    ts['isLinkedIn'] = ts.apply(isLinkedin, axis=1)

    ts.reset_index(drop=True, inplace=True)
    ts.to_json(fileDataName, orient='index')
    logger.info("Done.")

    return ts






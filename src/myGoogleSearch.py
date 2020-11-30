import pandas as pd
import time
import tldextract
import logging
import extract
from pathlib import Path
import config
import myLog
pathDataFiles=config.paths["data"]
from difflib import SequenceMatcher
import textdistance
from googlesearch import search
import lxml.html


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
        
def searchGoogle(df, searchedColumn, logger,fileDataName ):
    if "search" not in df.columns:
        df["search"] = "NOTYET"
    
    df['isSearched'] = df.apply(isSearched, axis=1)
    dfSize = len(df.index)
    nbToSearch = len(df[df['isSearched'] == False])
    logger.info(" %d / %d (to be searched / total)", nbToSearch, dfSize ) 
    
    for index, row in df[df['isSearched'] == False].iterrows():
        logger.info("%s (%d / %d)", row[searchedColumn], index+1, nbToSearch)
        df.at[index,'search'] = mySearch(row)
        if index % 10 == 0:
            logger.info("saved")
            df.to_json(fileDataName, orient='index')

    return df
def domains(row):
    ld = []    
    for item in row["search"]:
        d = tldextract.extract(item)
        dom = '.'.join(d[1:3])
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
def ownedDomain3(row, domainsToRemove=[]):
    rmax = 0
    r = 0
    owned="NONE"
    for search,searchTitle, domain in zip(row["search"], row["searchTitle"], row["domains"] ):
       
        if domain not in domainsToRemove:
            r = textdistance.ratcliff_obershelp(searchTitle.upper(), row['denomination'].upper())
        else:
            r = 0
        if r > rmax:
            rmax = r
            owned=search
    return owned


def calcOwnedDomains(df, logger,fileDataName ):

    logger.info("Begin calcOwnedDomains") 
    if "ownedDomain2" not in df.columns:
        df["ownedDomain2"] = "-"
    if "searchTitle" not in df.columns:
        df["searchTitle"] = "-"
    ld=[]
    for index, row in df.iterrows():
        for dom in row["domains"]:
            ld.append(dom)
            domainsToRemove=[]
    for d in ld:
        if ld.count(d) > 2:
            domainsToRemove.append(d)
    domainsToRemove = list(dict.fromkeys(domainsToRemove))
    logger.debug("Domains to remove %s", domainsToRemove)

    for index, row in df.iterrows():
        if row["searchTitle"] == "-":
            ts = []
            logger.debug("No title for %s -->let's search", row['denomination'])
            for s, d in zip(row["search"], row["domains"]):
                if d not in domainsToRemove:
                    try:
                        t = lxml.html.parse(s).find(".//title").text
                    except:
                        t="NOT FOUND"
                    logger.debug("titre : %s : %s", s, t)
                    ts.append(t)
        
            df.at[index, 'searchTitle']=ts
            df.at[index, 'ownedDomain3'] = ownedDomain3(row, domainsToRemove)
            df.to_json(fileDataName, orient='index')


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
                        logger.debug("On supprime %s %s (%r)", row['ownedDomain2'], row['denomination'].upper(), r )
                        df.at[index, 'ownedDomain2']="NONE"
            
            return df
        logger.debug("Loop %d , %d domains to remove : %s", loop,len(dTR), dTR.tolist() )
   
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






def go(dumpFile, logger):
    myLog.logIN(logger, dumpFile)
    fileDataName=pathDataFiles+"/"+dumpFile
    fileData = Path(fileDataName)
    if  not fileData.is_file() :
        raise NameError("FIle not found (%s)",fileDataName)
    else:
        ts = pd.read_json(fileDataName, orient='index')
        if 'search' not in ts.columns:
            ts['search'] = "NOTYET"
    
    ts["searchTitle"] = "-"


    logger.info("Data ready")
    ts=searchGoogle(ts, 'denomination', logger,dumpFile )
    ts.reset_index(drop=True, inplace=True)
    ts.to_json(fileDataName, orient='index')

    ts['domains'] = ts.apply(domains, axis=1)
    ts.reset_index(drop=True, inplace=True)

    ts['ownedDomain'] = ts.apply(ownedDomain, axis=1)
    ts['ownedDomain2'] = "-"
    ts=calcOwnedDomains(ts,logger,fileDataName)
    ts['scoreOwnedDomain'] = ts.apply(scoreOwnedDomain, axis=1)
    ts['scoreOwnedDomain2'] = ts.apply(scoreOwnedDomain2, axis=1)

    ts.reset_index(drop=True, inplace=True)

    ts['groupScoreOwned'] = pd.cut(ts['scoreOwnedDomain'], [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 1],include_lowest=True)
    

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






#!/home/ec2-user/dev/python_env/bin/python
# coding: utf8
from api_insee.criteria import Field, Periodic
from api_insee import ApiInsee
import getopt
import sys
import pandas as pd
import myGoogleSearch
import extract
import logging
import myInit
myInit.setup_logging()

logger = logging.getLogger()


def build(codeCommuneEtablissement="*",
          etatAdministratifUniteLegale="A",
          categorieJuridiqueUniteLegale="5*",
          activitePrincipaleUniteLegale="*",
          activitePrincipaleEtablissement="*",
          forceEco=False,
          forceWeb=False):
    """[summary]

    Args:
        codeCommuneEtablissement (str, optional): [description]. Defaults to "*".
        etatAdministratifUniteLegale (str, optional): [description]. Defaults to "A".
        categorieJuridiqueUniteLegale (str, optional): [description]. Defaults to "5*".
        activitePrincipaleUniteLegale (str, optional): [description]. Defaults to "*".
        activitePrincipaleEtablissement (str, optional): [description]. Defaults to "*".
        force (bool, optional): [description]. Defaults to False.

    Returns:
        [type]: [description]
    """

    file = extract.query(
        codeCommuneEtablissement=codeCommuneEtablissement,
        etatAdministratifUniteLegale=etatAdministratifUniteLegale,
        categorieJuridiqueUniteLegale=categorieJuridiqueUniteLegale,
        activitePrincipaleUniteLegale=activitePrincipaleUniteLegale,
        activitePrincipaleEtablissement=activitePrincipaleEtablissement,
        force=forceEco)

    myGoogleSearch.go(
        codeCommuneEtablissement=codeCommuneEtablissement,
        etatAdministratifUniteLegale=etatAdministratifUniteLegale,
        categorieJuridiqueUniteLegale=categorieJuridiqueUniteLegale,
        activitePrincipaleUniteLegale=activitePrincipaleUniteLegale,
        activitePrincipaleEtablissement=activitePrincipaleEtablissement,
        force=forceWeb)


try:
    opts, _ = getopt.getopt(sys.argv[1:], '-u-a-j-l-e', [
        'commune=', 'etat_administratif=', 'categrorie_juridique=',
        'activite_unite=', 'activite_etablissement='
    ])
except getopt.GetoptError:
    usage()
    sys.exit(2)
codeCommuneEtablissement = "*"
etatAdministratifUniteLegale = "A"
categorieJuridiqueUniteLegale = "5*"
activitePrincipaleUniteLegale = "*"
activitePrincipaleEtablissement = "*"
for opt, arg in opts:

    if opt in ('-u', '--commune'):
        codeCommuneEtablissement = arg
    if opt in ('-a', '--etat_administratif'):
        etatAdministratifUniteLegale = arg
    if opt in ('-j', '--categrorie_juridique'):
        categorieJuridiqueUniteLegale = arg
    if opt in ('-l', '--activite_unite'):
        activitePrincipaleUniteLegale = arg
    if opt in ('-e', '--activite_etablissement'):
        #logger.debug("arg e ", arg)
        activitePrincipaleEtablissement = arg

logging.debug(
    "codeCommuneEtablissement : %s, etatAdministratifUniteLegale : %s, categorieJuridiqueUniteLegale : %s, activitePrincipaleUniteLegale : %s, activitePrincipaleEtablissement : %s ",
    codeCommuneEtablissement, etatAdministratifUniteLegale,
    categorieJuridiqueUniteLegale, activitePrincipaleUniteLegale,
    activitePrincipaleEtablissement)
force = False

build(codeCommuneEtablissement=codeCommuneEtablissement,
      etatAdministratifUniteLegale=etatAdministratifUniteLegale,
      categorieJuridiqueUniteLegale=categorieJuridiqueUniteLegale,
      activitePrincipaleUniteLegale=activitePrincipaleUniteLegale,
      activitePrincipaleEtablissement=activitePrincipaleEtablissement,
      forceEco=False,
      forceWeb=True)

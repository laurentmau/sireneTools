import logging
import extract
import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None)

logging.basicConfig(format='%(asctime)s %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

params = {"codeCommuneEtablissement": "*","etatAdministratifUniteLegale": "A",
'categorieJuridiqueUniteLegale': "5*",
'activitePrincipaleUniteLegale': "16.10A", 'etablissementSiege':True}
file = extract.query(logger, params)

import myGoogleSearch
ts=myGoogleSearch.go(file, logger)
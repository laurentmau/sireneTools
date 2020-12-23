#!/home/ec2-user/dev/python_env/bin/python
# coding: utf8
import getopt
import sys
import logging
import mySecrets
secrets = mySecrets.getSecrets()

import myInit
myInit.setup_logging()

logger = logging.getLogger()

TRELLO_API_KEY = secrets['trello']['key']
TRELLO_API_SECRET = secrets['trello']['secret']
# TRELLO_OAUTH_TOKEN = "0fe62a4347f27cf856076a9c8a7be5b72045c450d62587aa13d8f4d7b45ed7a1"
# TRELLO_OAUTH_TOKEN_SECRET = "bf6ec75d5a728aa257c29d3f814e5f99"
TRELLO_OAUTH_TOKEN = ""
TRELLO_OAUTH_TOKEN_SECRET = ""

# try:
#     opts, _ = getopt.getopt(sys.argv[1:], '-u-a-j-l-e', [
#         'commune=', 'etat_administratif=', 'categrorie_juridique=',
#         'activite_unite=', 'activite_etablissement='
#     ])
# except getopt.GetoptError:
#     usage()
#     sys.exit(2)
# codeCommuneEtablissement = "*"
# etatAdministratifUniteLegale = "A"
# categorieJuridiqueUniteLegale = "5*"
# activitePrincipaleUniteLegale = "*"
# activitePrincipaleEtablissement = "*"
# for opt, arg in opts:

#     if opt in ('-u', '--commune'):
#         codeCommuneEtablissement = arg
#     if opt in ('-a', '--etat_administratif'):
#         etatAdministratifUniteLegale = arg
#     if opt in ('-j', '--categrorie_juridique'):
#         categorieJuridiqueUniteLegale = arg
#     if opt in ('-l', '--activite_unite'):
#         activitePrincipaleUniteLegale = arg
#     if opt in ('-e', '--activite_etablissement'):
#         #logger.debug("arg e ", arg)
#         activitePrincipaleEtablissement = arg

from trello import TrelloClient
# creds = None
# if os.path.exists('token.pickle'):
#     with open('token.pickle', 'rb') as token:
#         creds = pickle.load(token)
# if not creds or not creds.valid:
#     if creds and creds.expired and creds.refresh_token:
#         creds.refresh(Request())
#     else:
#         flow = InstalledAppFlow.from_client_secrets_file(
#             'credentials.json', SCOPES)
#         creds = flow.run_local_server(port=0)
#     # Save the credentials for the next run
#     with open('token.pickle', 'wb') as token:
#         pickle.dump(creds, token)

logger.debug("before client")
# client = TrelloClient(api_key=TRELLO_API_KEY,
#                       api_secret=TRELLO_API_SECRET,
#                       token=TRELLO_OAUTH_TOKEN,
#                       token_secret=TRELLO_OAUTH_TOKEN_SECRET)
client = TrelloClient(api_key=TRELLO_API_KEY, api_secret=TRELLO_API_SECRET)
logger.debug("after client")

all_boards = client.list_boards()
for b in all_boards:
    if b.name == "AuRorA-SYNCHRO":
        logger.info("BOARD : %s - - - - - - - - -- ", b.name)
        for l in b.list_lists():
            logger.info("\n_____________\nList : %s ", l.name)
            for card in l.list_cards():
                logger.info(card.name)

from google.cloud import datastore
from google.cloud import firestore

#client = datastore.Client()
#adding first data
db = firestore.Client()
doc_ref = db.collection('employee').document('empdoc')

doc_ref.set({'name': 'Parwiz', 'lname': 'Forogh', 'age': 24}, merge=True)

#adding second data
doc_ref = db.collection('employee').document('emptwodoc')
doc_ref.set(
    {
        'name': 'John3',
        'lname': 'Doe',
        'email': 'john@gmail.com',
        'age': 24
    },
    merge=True)

#Reading the data

emp_ref = db.collection('employee')
docs = emp_ref.stream()

for doc in docs:
    print('{} => {} '.format(doc.id, doc.to_dict()))

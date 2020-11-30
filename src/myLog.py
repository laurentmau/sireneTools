import inspect
def logIN(logger,msg=[]):
    logger.debug("IN --> %s %s", inspect.stack()[1].function,msg)


def logOUT(logger):
    logger.debug("OUT <-- %s", (inspect.stack()[1].function))
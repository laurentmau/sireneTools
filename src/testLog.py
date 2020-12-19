#!/home/ec2-user/dev/python_env/bin/python
# coding: utf8

import logging
import myLog
myLog.setup_logging()
logger = logging.getLogger(__name__)


logger.error("Error %s", __name__)
logger.info("Info")
logger.warning("Warning")
logger.debug("Debug")
from logging_tree import printout
printout()
import os
import json
import logging
import logging.config
logger = logging.getLogger(__name__)


def setup_logging(default_path='logging.json',
                  default_level=logging.DEBUG,
                  env_key='LOG_CFG'):
    path = default_path
    value = os.getenv(env_key, None)
    if value:
        path = value
    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
        logging.config.dictConfig(config)
    else:
        logging.basicConfig(level=default_level)


def getConfig(default_path='config.json', ):
    """Get configuration

    """
    path = default_path
    search = os.getenv("SEARCH_GOOGLE", None)

    if os.path.exists(path):
        with open(path, 'rt') as f:
            config = json.load(f)
            if search == "YES":
                config["googleSearch"] = "YES"
            else:
                config["googleSearch"] = "NO"
            return config

    else:
        logging.error("Config File not found (%s)", path)
        sys.exit(2)
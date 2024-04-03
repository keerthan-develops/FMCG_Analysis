from pathlib import Path
import logging
from setLogger import setLogger


# Create and configure logger
logger = logging.getLogger(__name__)
# Call the setLogger method to instantiate handler
logger_obj = setLogger(logger, 'resolvePath')
logger = logger_obj.set_handler()
logger.info('Check resolvePath')


class resolvePath:

    # init method or constructor
    def __init__(self):
        logger.info('transform object instantiated')
        
    # Sample Method
    @staticmethod
    def get_path():
        pwd = Path().resolve()
        
        repo_dir = str(pwd).replace('/src', '')
        repo_dir = Path(repo_dir).resolve()
        
        logger.debug(repo_dir)
        
        data_dir = Path(repo_dir) / 'data'
        logger.debug(data_dir)
        
        clp_path = str(data_dir / 'clp-places.json')
        logger.debug(Path(clp_path).is_file())
        
        cogo_path = str(data_dir / 'cogo-colpnts.json')
        logger.debug(Path(cogo_path).is_file())
        
        dats_path = str(data_dir / 'dats-places.json')
        logger.debug(Path(dats_path).is_file())
        
        okay_path = str(data_dir / 'okay-places.json')
        logger.debug(Path(okay_path).is_file())
        
        spar_path = str(data_dir / 'spar-places.json')
        logger.debug(Path(spar_path).is_file())
        
        log_path = Path(repo_dir) / 'logs'
        
        publish_dir = str(repo_dir / 'publish' / 'fmcg_analysis')
        logger.debug(Path(publish_dir).is_dir())

        return data_dir, clp_path, cogo_path, dats_path, okay_path, spar_path, log_path, publish_dir
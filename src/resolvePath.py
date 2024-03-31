from pathlib import Path
import logging
from setLogger import setLogger

class resolvePath:

    # Create and configure logger
    logger = logging.getLogger(__name__)
    # Call the setLogger method to instantiate handler
    logger_obj = setLogger(logger, 'resolvePath')
    logger = logger_obj.set_handler()
    logger.info('Check resolvePath')

    # init method or constructor
    def __init__(self):
        self.logger.info('transform object instantiated')
        
    # Sample Method
    def get_path(self):
        pwd = Path().resolve()
        
        self.repo_dir = str(pwd).replace('/src', '')
        self.repo_dir = Path(self.repo_dir).resolve()
        
        self.logger.debug(self.repo_dir)
        data_dir = Path(self.repo_dir) / 'data'
        self.logger.debug(data_dir)
        clp_path = str(data_dir / 'clp-places.json')
        self.logger.debug(Path(clp_path).is_file())
        cogo_path = str(data_dir / 'cogo-colpnts.json')
        self.logger.debug(Path(cogo_path).is_file())
        dats_path = str(data_dir / 'dats-places.json')
        self.logger.debug(Path(dats_path).is_file())
        okay_path = str(data_dir / 'okay-places.json')
        self.logger.debug(Path(okay_path).is_file())
        spar_path = str(data_dir / 'spar-places.json')
        self.logger.debug(Path(spar_path).is_file())
        log_path = Path(self.repo_dir) / 'logs'

        return data_dir, clp_path, cogo_path, dats_path, okay_path, spar_path, log_path
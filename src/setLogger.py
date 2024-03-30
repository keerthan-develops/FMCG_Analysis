import logging
from pathlib import Path

class setLogger:
    def __init__(self, logger, module):
        self.logger = logger
        self.module = module

    def set_handler(self):

        repo_dir = str(Path().resolve()).replace('/src', '')

        # Set the log level
        logging.basicConfig(level='INFO')

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        # Set the log file name
        log_file = str(Path(repo_dir) / 'logs' / 'assignment.log')

        # Create the handler
        if self.module == 'main':
            mode = 'w'
        else:
            mode = 'a'
        
        handler = logging.FileHandler(filename=log_file, mode=mode)

        # set the formatter to the handler created
        handler.setFormatter(formatter)

        # Add the handler to the logger
        self.logger.addHandler(handler)

        self.logger.info('Log test')

        return self.logger
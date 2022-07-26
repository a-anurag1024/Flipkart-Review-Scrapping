import logging


class my_logs:
    def __init__(self, handler):
        """ To initialize the log object"""
        self.level = logging.DEBUG
        self.message = '{} message'.format(self.level)
        self.err_des = 'No error description available'
        self.handler = handler

    def set_logger(self):
        """
        INPUT: None
        RETURNS: logger object with specified formatting
        INFO: Sets up the basic Configuration of the logger with the specified formatting
        """
        logger = logging.getLogger(self.handler)
        formatting = "%(asctime)s : %(name)s : %(levelname)s : %(message)s"
        logging.basicConfig(filename='logs.log', level=self.level, format=formatting)
        return logger

    def log_info(self, mes):
        """
        INPUT: mes
        RETURNS: None
        INFO: logs the input mes as the message in the info security level
        """
        self.level = logging.INFO
        self.message = mes
        logger = self.set_logger()
        logger.info(self.message)

    def log_error(self, mes, error):
        """
        INPUT: mes, error
        RETURNS: None
        INFO: logs the input mes as the message in the error security level and the detailed error is passed via error
        """
        self.level = logging.ERROR
        self.message = mes + "\n Exact Error: \n" + error
        logger = self.set_logger()
        logger.exception(self.message)      # Not using the logger.error as it only reports the heading of the error

    def log_warning(self, mes):
        """
        INPUT: mes
        RETURNS: None
        INFO: logs the input mes as the message in the warning security level
        """
        self.level = logging.WARNING
        self.message = mes
        logger = self.set_logger()
        logger.warning(self.message)


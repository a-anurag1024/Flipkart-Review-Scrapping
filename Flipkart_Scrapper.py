from selenium import webdriver

class Scrapper_Class:

    def __init__(self, log_info):
        """
        This function is used to initialize the selenium webdriver to operate the web browser. Here the firefox browser is used
        :param log_info- contains the path to log info for the firefox
        """
        try:
            self.driver = webdriver.Firefox(service = log_info)
        except Exception as e:
            raise Exception("Error occurred in initializing the webdriver. \n" + str(e))

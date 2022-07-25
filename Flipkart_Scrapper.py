from selenium import webdriver
from selenium.webdriver.common.by import By

from logger import my_logs
from page_locators import locator

logs = my_logs('Flipkart_Scrapper.py')


class Scrapper_Class:

    def __init__(self, log_info):
        """
        This function is used to initialize the selenium webdriver to operate the web browser. Here the firefox browser is used
        :param log_info- contains the path to log info for the firefox
        """
        try:
            self.driver = webdriver.Firefox(service = log_info)
        except Exception as e:
            logs.log_error("Error occurred in initializing the webdriver.", str(e))
            raise Exception("Error occurred in initializing the webdriver. \n" + str(e))

    def wait(self):
        """
        The function waits for the website to fully load till the given amount of time
        :return: None
        """
        try:
            self.driver.implicitly_wait(2)
        except Exception as e:
            logs.log_error("Error in the wait function: ", str(e))
            raise Exception("Error in wait function: " + str(e))

    def locate_Xpath(self, path):
        """
        The Function locates a web element by its Xpath
        :param path: The Xpath of the element
        :return: an object of class WebElement corresponding to the given web element
        """
        try:
            return self.driver.find_element(By.XPATH, value=path)
        except Exception as e:
            logs.log_error("Error in locating the given xpath", str(e))
            raise Exception("Error in locating the given xpath \n" + str(e))

    def open_url(self, url):
        """
        The function opens the given url in the webdriver
        :param url: the url to be opened
        :return: None
        """
        try:
            self.driver.get(url)
        except Exception as e:
            logs.log_error('Error in opening the url', str(e))
            raise Exception("Error in opening the url: " + str(e))

    def login_popup_cross(self):
        """
        The Function cancels the login popup which appears when the Flipkart webpage is hit
        :return: None
        """
        try:
            self.wait()
            loc = locator()
            self.locate_Xpath(loc.loc_loginPopup_close()).click()
        except Exception as e:
            logs.log_error("Error in cancelling the login popup:", str(e))
            raise Exception("Error in cancelling the login popup: \n", str(e))



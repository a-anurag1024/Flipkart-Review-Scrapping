class locator:
    def __init__(self):
        pass

    def loc_loginPopup_close(self):
        """
        locates the close button of the login popup
        :return: XPath to the close button of the login button
        """
        return "//body[1]/div[2]/div[1]/div[1]/button[1]"

    def loc_search_field(self):
        """
        locates the search field in the flipkart homepage
        :return: XPath to the search field
        """
        return "/html[1]/body[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/form[1]/div[1]/div[1]/input[1]"

    def loc_search_button(self):
        """
        locates the search button in the flipkart homepage
        :return: XPath to the search field
        """
        return "/html[1]/body[1]/div[1]/div[1]/div[1]/div[1]/div[2]/div[2]/form[1]/div[1]/button[1]"

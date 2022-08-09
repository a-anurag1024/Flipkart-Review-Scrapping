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

    def loc_product_type(self):
        """
        locates the type of the product in the product page
        :return: XPath to the product type
        """
        return "/html[1]/body[1]/div[1]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/div[1]/div[2]/a[1]"

    def loc_product_price(self):
        """
        locate the advertised price of the product in the product page
        :return: class name of the product_price
        """
        return "_30jeq3._16Jk6d"

    def loc_product_name(self):
        """
        locate the name of the product in the product page
        :return: class name of the product_name
        """
        return "B_NuCI"

    def loc_product_MRP(self):
        """
        locate the MRP of the product in the product page
        :return: class name of the product_MRP
        """
        return "_3I9_wc._2p6lqe"


    def loc_first_review_permalink(self):
        """
        locate the permalink of the first review of the product in the product page
        :return: class name of the permalink
        """
        return "_3XCI6U"

    def loc_product_review_page(self):
        """
        locate the product review page link from the fisrt comment permalink page
        :return: CLass of the product_review_page
        """
        return "_2yVg4P"

    def loc_rating_t1(self):
        """
        locate the rating in type-1 review page
        :return: class of the rating
        """
        return "_2d4LTz"

    def loc_rating_t2(self):
        """
        locate the rating in type-2 review page
        :return: class of the rating
        """
        return "_3LWZlK._12yO4d"

    def loc_num_rate_rev_t1(self):
        """
        locates the number of ratings and reviews in type-1 review page
        :return: list of format [XPath_of_ratings, XPath_of_reviews]
        """
        qr = "/html[1]/body[1]/div[1]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[1]"
        return [qr+"/div[2]/div[1]", qr+"/div[3]/div[1]"]

    def loc_num_rate_rev_t2(self):
        """
                locates the number of ratings and reviews (given in single line) in type-2 review page
                :return: class of the ratings and reviews
                """
        return "_3zoWhv"

    def loc_rating_ratings_t1(self):
        """
        locates the number of individual ratings in review page type1
        :return: XPath of the ratings in list format [5, 4, 3, 2, 1]
        """
        qr = "/html[1]/body[1]/div[1]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/div[1]/div[2]/div[1]/ul[3]"
        return [qr+"/li[1]/div[1]", qr+"/li[2]/div[1]", qr+"/li[3]/div[1]", qr+"/li[4]/div[1]", qr+"/li[5]/div[1]"]

    def loc_rating_ratings_t2(self):
        """
        locates the number of individual ratings in review page type2
        :return: XPath of the ratings in list format [5, 4, 3, 2, 1]
        """
        qr = "/html[1]/body[1]/div[1]/div[1]/div[3]/div[1]/div[1]/div[2]/div[2]/div[1]/div[1]/div[2]/div[1]/ul[3]"
        return [qr+"/li[1]/div[1]", qr+"/li[2]/div[1]", qr+"/li[3]/div[1]", qr+"/li[4]/div[1]", qr+"/li[5]/div[1]"]

    def loc_comment_table(self):
        """
        locates the table containing the comments
        :return: class of the comment table
        """
        return "_1AtVbE.col-12-12"

    def loc_comment_head_t1(self):
        """
        locates the heading of the comment in type-1 products page
        :return: Class of the heading
        """
        return "_2-N8zT"

    def loc_comment_comment_t2(self):
        """
        locates the comment in type-2 products page
        :return: Class of the heading
        """
        return "_6K-7Co"

    def loc_comment_comment_t1(self):
        """
        locates the content of the comment in type-1 products page
        :return: XPATH of the comment box
        """
        return "./div[1]/div[1]/div[1]/div[2]/div[1]/div[1]/div[1]"

    def loc_comment_rating_t1(self):
        """
        locates the rating of the comment review in type-1 product pages
        :return: class of the rating
        """
        return "_3LWZlK._1BLPMq"

    def loc_comment_rating_t2(self):
        """
        locates the rating of the comment review in type-2 product pages
        :return: class of the rating
        """
        return "_3LWZlK._1BLPMq._388WaH"

    def loc_comment_location(self):
        """
        locates the location of the comment
        :return: class of the location text
        """
        return "_2mcZGG"

    def loc_comment_month_exact_t1(self):
        """
        locates the month (exact) of the comment in the type-1 product page
        :return: XPATH of the month text
        """
        return "./div[1]/div[1]/div[1]/div[3]/div[1]/p[3]"

    def loc_comment_month_rel_t2(self):
        """
        locates the relative month of the comment in the type-1 product page
        :return: XPATH of the month text
        """
        return "./div[1]/div[1]/div[1]/div[4]/div[1]/p[3]"

    def loc_comment_age_t2(self):
        """
        locates the age of the comment in the type-2 product page
        :return: class of the age text
        """
        return "_2sc7ZR"
    def loc_comment_with_photo_like_dislike_t1(self):
        """
        locates the likes and dislikes of the comments with photos for product type 1
        :return: XPATH of the likes and dislikes in a list format [likes, dislikes]
        """
        qr = "./div[1]/div[1]/div[1]/div[4]/div[2]/div[1]/div[1]"
        return [qr+"/div[1]", qr+"/div[2]"]

    def loc_comment_without_photo_like_dislike_t1(self):
        """
        locates the likes and dislikes of the comments without photo for product page 1
        :return: XPATH of the likes and dislikes in a list format [likes, dislikes]
        """
        qr = "./div[1]/div[1]/div[1]/div[3]/div[2]/div[1]/div[1]"
        return [qr+"/div[1]", qr+"/div[2]"]

    def loc_comment_like_dislike_t2(self):
        """
        locates the likes and dislikes of the comments for product page 2. Only two elements in each comment class which belong to this class.
        And they are the likes and dislikes counters. The like counter comes before dislike counter

        :return: class of the likes and dislikes.
        """
        return "_3c3Px5"

    def loc_next_page_button_t1(self):
        """
        locates the next page of the comments in the type-1 product page
        :return: Class of the next page button
        """
        return "_1LKTO3"

    def loc_next_page_button_t2(self):
        """
        locates the previous/next page of the comments in the type-2 product page
        :return: Class of the next page button
        """
        return "_1LKTO3"
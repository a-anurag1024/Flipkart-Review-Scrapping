from selenium import webdriver
from selenium.webdriver.common.by import By
from webdriver_manager.firefox import GeckoDriverManager
from selenium.webdriver.firefox.service import Service as FirefoxService
import pandas as pd

from logger import my_logs
from page_locators import locator

from database import cassandra_db

logs = my_logs('Flipkart_Scrapper.py')


class Scrapper_Class:

    def __init__(self):
        """
        This function is used to initialize the selenium webdriver to operate the web browser. Here the firefox browser is used
        """
        try:
            self.driver = webdriver.Firefox(service=FirefoxService(GeckoDriverManager().install()))
            logs.log_info("Firefox web driver has been set up.")
        except Exception as e:
            logs.log_error("Error occurred in initializing the webdriver.", str(e))
            raise Exception("Error occurred in initializing the webdriver. \n" + str(e))

    def wait(self):
        """
        The function waits for the website to fully load till the given amount of time
        :return: None
        """
        try:
            self.driver.implicitly_wait(3)
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
            logs.log_info("Accessed URL {}".format(url))
        except Exception as e:
            logs.log_error('Error in opening the url', str(e))
            raise Exception("Error in opening the url: " + str(e))

    def login_popup_cross(self):
        """
        The Function cancels the login popup which appears when the Flipkart webpage is hit
        :return: None
        """
        try:

            loc = locator()
            self.locate_Xpath(loc.loc_loginPopup_close()).click()
            logs.log_info("Login pop-up canceled")
        except Exception as e:
            logs.log_error("Error in cancelling the login popup:", str(e))
            raise Exception("Error in cancelling the login popup: \n", str(e))

    def product_search(self, searchString):
        """
        Function to search the given product in the Flipkart webpage through the webdriver
        :param searchString: product to be searched
        :return: None
        """
        try:
            loc = locator()
            self.locate_Xpath(loc.loc_search_field()).send_keys(searchString)
            self.driver.find_element(By.XPATH, loc.loc_search_button()).click()
            logs.log_info("Products searched for query {}".format(searchString))
        except Exception as e:
            logs.log_error('Error in typing in the search bar of the Flipkart homepage: ', str(e))
            raise Exception('Error in typing in the search bar of the Flipkart homepage: ' + str(e))

    def get_product_links(self):
        """
        Function to get the product links displayed in the flipkart webpage
        :return: list of product links in the most relevant order they appear in the Flipkart search
        """
        try:
            cur_url = self.driver.current_url
            self.driver.get(cur_url)
            links_obs = self.driver.find_elements(By.TAG_NAME, 'a')  # all the links obtained in that page
            p_links = []
            for i in range(len(links_obs)):
                if '?pid' in links_obs[i].get_attribute('href'):
                    p_links.append(
                        links_obs[i].get_attribute('href'))  # links having '?pid' only correspond to products
            unique_p_links = []
            for i in p_links:
                if i not in unique_p_links:
                    unique_p_links.append(i)
            logs.log_info("Product links obtained for the searched query.")
            return unique_p_links
        except Exception as e:
            logs.log_error('Error in obtaining product links.', str(e))
            raise Exception('Error in obtaining product links.\n' + str(e))

    def get_review_page(self, plink):
        """
        Function to open the product review page given the product link
        :param plink: link to the product page
        :return: 0 if no review page available else None
        """
        try:
            loc = locator()
            self.open_url(plink)
            try:
                loc_ele = self.driver.find_element(By.CLASS_NAME, value=loc.loc_first_review_permalink())
            except:
                logs.log_warning("The current product has no comments.")
                return 0
            loc_ele = loc_ele.find_element(By.TAG_NAME, value='a')
            self.open_url(loc_ele.get_attribute('href'))
            loc_ele = self.driver.find_element(By.CLASS_NAME, value=loc.loc_product_review_page())
            try:
                self.open_url(loc_ele.get_attribute('href'))
            except:
                loc_ele.click()
            logs.log_info("Product review page opened.")
            return
        except Exception as e:
            logs.log_error('Error in obtaining product review page.', str(e))
            raise Exception('Error in obtaining product review page.\n' + str(e))

    def fetch_product_details(self, plink):
        """
        Function to obtain the various product details.
        :param plink: hyperlink of the product page
        :return: list of product details in format [pname, ptag, pprice, pmrp, no_rate, no_reviews, agg_rate, ratings]
        """
        try:
            loc = locator()
            self.open_url(plink)
            # product name
            loc_ele = self.driver.find_element(By.CLASS_NAME, value=loc.loc_product_name())
            pname = loc_ele.text
            # product tag
            loc_ele = self.driver.find_element(By.XPATH, value=loc.loc_product_type())
            ptag = loc_ele.text
            # product price
            loc_ele = self.driver.find_element(By.CLASS_NAME, value=loc.loc_product_price())
            pprice = float(loc_ele.text[1:].replace(',', ''))
            # product mrp
            loc_ele = self.driver.find_element(By.CLASS_NAME, value=loc.loc_product_MRP())
            pmrp = float(loc_ele.text[1:].replace(',', ''))
            flag = self.get_review_page(plink)
            if flag == 0:
                no_rate = None
                no_reviews = 0
                agg_rate = None
                ratings = None
                return [pname, ptag, pprice, pmrp, no_rate, no_reviews, agg_rate, ratings]
            try:  # For page-type of 1st kind
                # finding the product's number of ratings
                loc_ele = self.driver.find_element(By.XPATH,
                                                   value=loc.loc_num_rate_rev_t1()[0])
                no_rate = int(loc_ele.text[:-9].replace(",", ""))
                # finding the product's number of reviews
                loc_ele = self.driver.find_element(By.XPATH,
                                                   value=loc.loc_num_rate_rev_t1()[1])
                no_reviews = int(loc_ele.text[:-7].replace(",", ""))
                # finding the aggregate rating
                loc_ele = self.driver.find_element(By.CLASS_NAME,
                                                   value=loc.loc_rating_t1())
                agg_rate = float(loc_ele.text.replace(",", ""))
                # finding the number of each ratings [5-star, 4-star, 3-star, 2-star, 1-star]
                ratings = []
                for i in range(5):
                    loc_ele = self.driver.find_element(By.XPATH,
                                                       value=loc.loc_rating_ratings_t1()[i])
                    ratings.append(int(loc_ele.text.replace(",", "")))
            except:  # For page type of second kind (in clothings)
                loc_ele = self.driver.find_element(By.CLASS_NAME,
                                                   value=loc.loc_num_rate_rev_t2())
                no_rate = int(loc_ele.text.split(' ')[0].replace(",", ""))
                no_reviews = int(loc_ele.text.split(' ')[3].replace(",", ""))
                loc_ele = self.driver.find_element(By.CLASS_NAME,
                                                   value=loc.loc_rating_t2())
                agg_rate = float(loc_ele.text)
                ratings = []
                for i in range(5):
                    loc_ele = self.driver.find_element(By.XPATH,
                                                       value=loc.loc_rating_ratings_t2()[i])
                    ratings.append(int(loc_ele.text.replace(",", "")))
            return [pname, ptag, pprice, pmrp, no_rate, no_reviews, agg_rate, ratings]
        except Exception as e:
            logs.log_error('Error in fetching data from the product and review page.', str(e))
            raise Exception('Error in fetching data from the product and review page.\n' + str(e))

    def fetch_product_comments(self, plink, n_com, r_page=True):
        """
        Function to fetch the comments from a given product.
        :param plink: product link
        :param n_com: Max number of comments to be extracted
        :param r_page: if the driver is in the reviews page
        :return: dataframe containing the comments information
        """
        try:
            if not r_page:
                self.get_review_page(plink)
            counter = 0  # number of reviews traversed
            loc = locator()
            df = pd.DataFrame(columns=['title', 'comment', 'rating', 'month', 'location', 'likes', 'dislikes'])
            while counter < n_com:
                page_ele = self.driver.find_elements(By.CLASS_NAME, value=loc.loc_comment_table())
                page_count = 4  # number of comments collected in the given comment page
                while len(page_ele) - 1 > page_count and counter < n_com:
                    vals = []
                    loc_ele = page_ele[page_count]
                    # to find the comment heading
                    try:
                        vals.append(loc_ele.find_element(By.CLASS_NAME, value=loc.loc_comment_head_t1()).text)
                    except:
                        vals.append(None)
                    # to find the comment text
                    try:
                        vals.append(loc_ele.find_element(By.XPATH, value=loc.loc_comment_comment_t1()).text)
                    except:
                        try:
                            vals.append(loc_ele.find_element(By.CLASS_NAME, value=loc.loc_comment_comment_t2()).text)
                        except:
                            vals.append(None)
                    # to find the comment rating
                    try:
                        vals.append(float(loc_ele.find_element(By.CLASS_NAME, value=loc.loc_comment_rating_t1()).text))
                    except:
                        try:
                            vals.append(
                                float(loc_ele.find_element(By.CLASS_NAME, value=loc.loc_comment_rating_t2()).text))
                        except:
                            vals.append(None)
                    # to find the comment month
                    try:
                        vals.append((loc_ele.find_elements(By.CLASS_NAME, value=loc.loc_comment_age_t2())[-1]).text)
                    except:
                        try:
                            vals.append(loc_ele.find_element(By.XPATH, value=loc.loc_comment_month_exact_t1()).text)
                        except:
                            try:
                                vals.append(loc_ele.find_element(By.XPATH, value=loc.loc_comment_month_rel_t2()).text)
                            except:
                                vals.append(None)
                    # to find the comment location
                    try:
                        vals.append(
                            ((loc_ele.find_element(By.CLASS_NAME, value=loc.loc_comment_location()).text).split(','))[
                                -1])
                    except:
                        vals.append(None)
                    # to find the comment likes and dislikes
                    try:
                        vals.append(
                            loc_ele.find_element(By.XPATH, value=loc.loc_comment_with_photo_like_dislike_t1()[0]).text)
                        vals.append(
                            loc_ele.find_element(By.XPATH, value=loc.loc_comment_with_photo_like_dislike_t1()[1]).text)
                    except:
                        try:
                            vals.append(
                                loc_ele.find_element(By.XPATH,
                                                     value=loc.loc_comment_without_photo_like_dislike_t1()[0]).text)
                            vals.append(
                                loc_ele.find_element(By.XPATH,
                                                     value=loc.loc_comment_without_photo_like_dislike_t1()[1]).text)
                        except:
                            try:
                                vals.append(
                                    loc_ele.find_elements(By.CLASS_NAME, value=loc.loc_comment_like_dislike_t2())[
                                        0].text)
                                vals.append(
                                    loc_ele.find_elements(By.CLASS_NAME, value=loc.loc_comment_like_dislike_t2())[
                                        1].text)
                            except:
                                vals.append(None)
                                vals.append(None)
                    df.loc[len(df)] = vals
                    counter += 1
                    page_count += 1
                    print(vals)
                    print(page_count, counter)
                try:
                    print('hit new page option-1')
                    self.driver.find_elements(By.CLASS_NAME, value=loc.loc_next_page_button_t1())[-1].click()
                except:
                    try:
                        print('hit new page option-2')
                        self.driver.find_elements(By.CLASS_NAME, value=loc.loc_next_page_button_t2())[-1].click()
                    except:
                        print('hit new page option-3')
                        logs.log_warning(
                            "No new pages of comments for the product. Need to extract {} product comments from other products".format(
                                n_com - counter))
                        break
            return df

        except Exception as e:
            logs.log_error('Error in obtaining product comments from the review page.', str(e))
            raise Exception('Error in obtaining product comments from the review page.\n' + str(e))

    def establish_searched_products_table(self):
        """
        Function to establish the table containing all the details of the products searched.
        :return: None
        """
        try:
            cdb = cassandra_db()
            cdb.connect_db()
            cdb.establish_keyspace('reviews')   # name of the keyspace is reviews
            t = cdb.list_tables()
            if 'products_searched' not in t:
                columns = "search_id int PRIMARY KEY, search_string text, product_name text, product_tag text, product_price float, product_MRP float, no_of_ratings int, no_of_reviews int, overall_rating float, no_of_5stars int, no_of_4stars int, no_of_3stars int, no_of_2stars int, no_of_1star int"
                table_name = 'products_searched'
                cdb.establish_table(table_name, columns)
                logs.log_warning("Products_searched table was not present in the keyspace. Initialized the products_searched table.")
                return
            else:
                logs.log_info("Products_searched table found in the keyspace.")
        except Exception as e:
            logs.log_error('Error in searched product table in the database.', str(e))
            raise Exception('Error in searched product table in the database.\n' + str(e))

    def get_last_search_id(self):
        """
        Function to get the last search id in the products_searched table
        :return: search_id of the last search
        """
        try:
            dbc = cassandra_db()
            self.establish_searched_products_table()
            query = "select search_id, search_string from reviews.products_searched; "
            a = dbc.read_table(query)
            max_a = a.loc[a.search_id == a['search_id'].max()]
            last_search_id = max_a['search_id']
            return last_search_id
        except Exception as e:
            logs.log_error('Error in finding the last search id in the products_searched table in the database.', str(e))
            raise Exception('Error in finding the last search id in the products_searched table in the database.\n' + str(e))

    def check_db(self, search_str):
        """
        Function to check if the result for the search string already exists in the last 10 search results.
        :param search_str: the search string asked for searching the product
        :return: search_id of the table containing the comments (if exists), False otherwise.
        """
        try:
            dbc = cassandra_db()
            self.establish_searched_products_table()
            query = "select search_id from reviews.products_searched where search_string = '{}' ALLOW FIlTERING".format(
                search_str.lower())
            search_ids = dbc.read_table(query)
            if len(search_ids) != 0:
                ret_ids = []
                if len(search_ids) == 1:
                    table_name = "search_no_{}".format(search_ids)
                    if dbc.is_table_present(table_name):
                        return search_ids
                    else:
                        return False
                else:
                    for i in range(len(search_ids)):
                        table_name = "search_no_{}".format(search_ids[i])
                        if dbc.is_table_present(table_name):
                            ret_ids.append(i)
                    if len(ret_ids) != 0:
                        return ret_ids
                    else:
                        return False
            else:
                return False
        except Exception as e:
            logs.log_error('Error in searching for the given search string in the database.', str(e))
            raise Exception('Error in searching for the given search string in the database.\n' + str(e))

    def search_and_get_product_links(self, search_str):
        """
        Function to search and obtain the product links.
        :param search_str: search string
        :return: list of all the product links found
        """
        self.open_url('https://www.flipkart.com')
        self.login_popup_cross()
        self.product_search(search_str)
        links = self.get_product_links()
        return links

    def create_new_search_table(self, search_str, no_comments, skip=0):
        """
        Function to create a new search result table in the database.
        :param search_str: string to be searched
        :param no_comments: number of comments to be scrapped
        :param skip: number of products in the search result to be skipped (scrapped by earlier instances)
        :return: search_ids of the new table(s) created
        """
        try:
            cdb = cassandra_db()
            cdb.connect_db()
            l = cdb.list_tables()
            nos = []
            for i in l:
                if i[:10] == 'search_no_':
                    nos.append(int(i[10:]))
            sno = max(nos) + 1
            comments_counter = 0
            plinks = self.search_and_get_product_links(search_str)
            i = skip
            while comments_counter < no_comments:
                p_details = self.fetch_product_details(plinks[i])
                values = {'search_id': sno, 'search_string': search_str, 'product_name': p_details[0],
                          'product_tag': p_details[1], 'product_price': p_details[2], 'product_MRP': p_details[3],
                          'no_of_ratings': p_details[4], 'no_of_reviews': p_details[5], 'overall_rating': p_details[6],
                          'no_of_5stars': p_details[7][0], 'no_of_4stars': p_details[7][1], 'no_of_3stars': p_details[7][2],
                          'no_of_2stars': p_details[7][3], 'no_of_1star': p_details[7][4]}
                cdb.table = 'products_searched'
                cdb.add_single_data(values)
                tname = "search_no_{}".format(sno)
                cols = "comment_no int PRIMARY KEY, title text, comment text, rating float, month text, location text, likes int, dislikes int"
                cdb.establish_table(tname, cols)
                df = self.fetch_product_comments(plinks[i], no_comments - comments_counter, r_page=False)
                df['comment_no'] = df.index
                cdb.add_batch_data(df)
                comments_counter += len(df)
                sno += 1
                i += 1
        except Exception as e:
            logs.log_error('Error in creating a new search table.', str(e))
            raise Exception('Error in creating a new search table.\n' + str(e))

    def get_comments(self, search_str, no_comments):
        """
        Function to get the product comment details of the searched product in the database
        :param search_str: the search string asked for searching the product
        :param no_comments: number of comments to be searched
        :return list of dataframes from different scrapped products of the searched string
        """
        try:
            cdb = cassandra_db()
            cdb.connect_db()
            # to check existing records
            search_ids = self.check_db(search_str)
            no_exits_com = 0
            dfs = []
            if search_ids:
                for i in search_ids:
                    query = "select * from reviews.search_no_{};".format(i)
                    dfs.append(cdb.read_table(query))
                    no_exits_com += len(dfs[-1])
            # case-1 : when no extra data scrapping is required
            if no_exits_com < no_comments:
                return dfs
            # case-2 : when existing data is not enough
            else:
                dfs.append(self.create_new_search_table(search_str, no_comments - no_exits_com, len(dfs)))
                return dfs
        except Exception as e:
            logs.log_error('Error in returning the search results to the App.', str(e))
            raise Exception('Error in returning the search results to the App.\n' + str(e))

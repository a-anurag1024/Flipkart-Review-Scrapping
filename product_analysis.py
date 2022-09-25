import numpy as np

from database import cassandra_db
from logger import my_logs
import plotly.express as px

logs = my_logs('product_analysis.py')


class product_analytics:
    def __init__(self):
        """
        Function to initialize the connection with the cassandra database.
        """
        try:
            self.cdb = cassandra_db()
            self.cdb.connect_db()
            self.cdb.establish_keyspace('reviews')
            self.df = None
        except Exception as e:
            logs.log_error("Error occurred in initializing the database connection", str(e))
            raise Exception("Error occurred in initializing the database connection. \n" + str(e))

    def establish_searched_products_table(self):
        """
        Function to establish the table containing all the details of the products searched.
        :return: None
        """
        try:
            self.cdb.establish_keyspace('reviews')  # name of the keyspace is reviews
            t = self.cdb.list_tables()
            if 'products_searched' not in t:
                columns = "search_id int PRIMARY KEY, search_string text, product_name text, product_tag text, product_price float, product_MRP float, no_of_ratings int, no_of_reviews int, overall_rating float, no_of_5stars int, no_of_4stars int, no_of_3stars int, no_of_2stars int, no_of_1star int"
                table_name = 'products_searched'
                self.cdb.establish_table(table_name, columns)
                logs.log_warning(
                    "Products_searched table was not present in the keyspace. Initialized the products_searched table.")
                return
            else:
                logs.log_info("Products_searched table found in the keyspace.")
                self.cdb.table = 'reviews'
                return
        except Exception as e:
            logs.log_error('Error in searched product table in the database.', str(e))
            raise Exception('Error in searched product table in the database.\n' + str(e))

    def read_product_table(self):
        """
        Function to read the existing entries in the table containing info on all the products searched
        :return: None
        """
        try:
            self.establish_searched_products_table()
            query = "select * from reviews.products_searched; "
            self.df = self.cdb.read_table(query)
        except Exception as e:
            logs.log_error('Error in reading the products table in the database.', str(e))
            raise Exception('Error in reading the products table in the database.\n' + str(e))

    def total_searches(self):
        """
        Function to return the total number of searches made so far using the app
        :return: number of searches made so far
        """
        try:
            return len(self.df)
        except Exception as e:
            logs.log_error('Error in finding the total number of searches in the database.', str(e))
            raise Exception('Error in finding the total number of searches in the database.\n' + str(e))

    def find_most_searched_category(self):
        """
        Function to return the most searched category.
        :return: string specifying the most searched category
        """
        try:
            count_prod_cat = self.df[['search_id', 'product_tag']].groupby(['product_tag']).count()
            fig = px.bar(x=count_prod_cat.index, y=count_prod_cat['search_id'], labels=dict(x="Product Categories", y="Number of searches"))
            fig.write_image("plots/searches_per_category.png")
            return count_prod_cat.loc[count_prod_cat['search_id'] == count_prod_cat['search_id'].max()].index[0]
        except Exception as e:
            logs.log_error('Error in finding the products searched per product category.', str(e))
            raise Exception('Error in finding the products searched per product category.\n' + str(e))

    def reviews_per_category(self):
        """
        Function to plot the number of ratings per category and to return the highest reviewed category
        :return: highest reviewed category
        """
        try:
            count_prod_cat = self.df[['no_of_ratings', 'product_tag']].groupby(['product_tag']).sum()
            fig = px.bar(x=count_prod_cat.index, y=count_prod_cat['no_of_ratings'],
                         labels=dict(x="Product Categories", y="Number of Ratings"))
            fig.write_image("plots/no_of_ratings_per_category.png")
            return count_prod_cat.loc[count_prod_cat['no_of_ratings'] == count_prod_cat['no_of_ratings'].max()].index[0]
        except Exception as e:
            logs.log_error('Error in finding the no_of_ratings per product category.', str(e))
            raise Exception('Error in finding the no_of_ratings per product category.\n' + str(e))

    def get_success_of_product_category(self):
        """
        Function to plot avg ratings per category and to return the highest and lowest rated categories
        :return: (highest rated category, lowest rated category)
        """
        try:
            count_prod_cat = self.df[['overall_rating', 'product_tag']].groupby(['product_tag']).mean()
            fig = px.bar(x=count_prod_cat.index, y=count_prod_cat['overall_rating'],
                         labels=dict(x="Product Categories", y="Average rating"))
            fig.write_image("plots/avg_rating_per_category.png")
            max_r = count_prod_cat.loc[count_prod_cat['overall_rating'] == count_prod_cat['overall_rating'].max()].index[0]
            min_r = count_prod_cat.loc[count_prod_cat['overall_rating'] == count_prod_cat['overall_rating'].min()].index[0]
            return max_r, min_r
        except Exception as e:
            logs.log_error('Error in finding the average rating per product category.', str(e))
            raise Exception('Error in finding the average rating per product category.\n' + str(e))

    def max_discounted_product_category(self):
        """
        Function to find out the avg discount in each of the product category
        :return: max discounted product category
        """
        try:
            df1 = self.df[['product_tag', 'product_price', 'product_mrp']]
            df1['discount'] = ((df1['product_mrp']-df1['product_price'])/df1['product_mrp'])*100
            count_prod_cat = df1[['discount', 'product_tag']].groupby(['product_tag']).mean()
            fig = px.bar(x=count_prod_cat.index, y=count_prod_cat['discount'],
                         labels=dict(x="Product Categories", y="Average discount"))
            fig.write_image("plots/avg_discount_per_category.png")
            return count_prod_cat.loc[count_prod_cat['discount'] == count_prod_cat['discount'].max()].index[0]
        except Exception as e:
            logs.log_error('Error in finding the average discount per product category.', str(e))
            raise Exception('Error in finding the average discount per product category.\n' + str(e))

    def get_BnW_thinking(self):
        """
        Function to calculate the black and white metric per product category.
        :return: None
        """
        try:
            df1 = self.df[['product_tag', 'no_of_5stars', 'no_of_4stars', 'no_of_3stars', 'no_of_2stars', 'no_of_1star']]
            df1['bnw'] = (1/abs(df1['no_of_5stars']-df1['no_of_1star']))*(1/(df1['no_of_2stars']+df1['no_of_3stars']+df1['no_of_4stars']))*(df1['no_of_5stars']+df1['no_of_2stars']+df1['no_of_3stars']+df1['no_of_4stars']+df1['no_of_1star'])**2
            count_prod_cat = df1[['bnw', 'product_tag']].groupby(['product_tag']).mean()
            fig = px.bar(x=count_prod_cat.index, y=count_prod_cat['bnw'],
                         labels=dict(x="Product Categories", y="Average black and white thinking metric"))
            fig.write_image("plots/avg_bnw_per_category.png")
        except Exception as e:
            logs.log_error('Error in finding the black and white metric per product category.', str(e))
            raise Exception('Error in finding the black and white metric per product category.\n' + str(e))

    def product_stats(self):
        """
        Function to return all the product stats computed on the gathered product data
        :return: (total_searches, most_searched, max_reviews, max_rated, least_rated, max_discount)
        """
        try:
            self.read_product_table()
            total_searches = self.total_searches()
            most_searched = self.find_most_searched_category()
            max_rev = self.reviews_per_category()
            max_rated, least_rated = self.get_success_of_product_category()
            max_discount = self.max_discounted_product_category()
            self.get_BnW_thinking()
            return total_searches, most_searched, max_rev, max_rated, least_rated, max_discount
        except Exception as e:
            logs.log_error('Error in creating the product statistics.', str(e))
            raise Exception('Error in creating the product statistics.\n' + str(e))

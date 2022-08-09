from logger import my_logs
from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
from Flipkart_Scrapper import Scrapper_Class


rows = {}
collection_name = None

logs = my_logs('apps.py')

free_status = True
db_name = 'Flipkart_Review_Scrapping'

app = Flask(__name__)  # to initialize the flask app


@app.route('/', method=['Post', 'Get'])
@cross_origin()
def index():
    if request.method == 'POST':
        try:
            searchString = request.form['content'].replace(" ", "")  # getting the search query from the html form
            expected_review = int(request.form['expected_review'])   # getting the number of reviews from the html form
            logs.log_info("Obtained the query details from the user")
        except Exception as e:
            logs.log_error("Error in obtaining the input from the user:", str(e))
            searchString = 'macbookair'
            expected_review = 10
            logs.log_info("Taken dummy values for searchString and expected_review")
        try:
            scrapper = Scrapper_Class()
            scrapper.open_url("https://www.flipkart.com/")
            logs.log_info('The Flipkart home page has been hit.')
            scrapper.login_popup_cross()
            logs.log_info('The login popup cancelled.')
            scrapper.product_search(searchString)   # to search the product in the webdriver which opens up the results page
            logs.log_info('The product {} searched in the flipkart homepage search engine'.format(searchString))
        except Exception as e:
            pass
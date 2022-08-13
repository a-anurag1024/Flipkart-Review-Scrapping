import Flipkart_Scrapper
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
            expected_review = int(request.form['expected_review'])  # getting the number of reviews from the html form
            logs.log_info("Obtained the query details from the user")
        except Exception as e:
            logs.log_error("Error in obtaining the input from the user:", str(e))
            searchString = 'macbookair'
            expected_review = 10
            logs.log_info("Taken dummy values for searchString and expected_review")
        try:
            scrapper = Flipkart_Scrapper.Scrapper_Class()

        except Exception as e:
            logs.log_error(
                "Error in finding the search result either in the Database or saving a new instance to the database:",
                str(e))
            raise Exception(
                'Error in finding the search result either in the Database or saving a new instance to the database.\n' + str(
                    e))

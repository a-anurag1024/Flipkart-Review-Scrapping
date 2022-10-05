from flask import Flask, render_template, request, jsonify, Response, url_for, redirect, send_file
from flask_cors import CORS, cross_origin
import pandas as pd
from Flipkart_Scrapper import Scrapper_Class
from product_analysis import product_analytics
import sys
import time
import logging
import threading
from logger import my_logs

logs = my_logs('app.py')

app = Flask(__name__, template_folder="templates")

app.logger.addHandler(logging.StreamHandler(sys.stdout))
app.logger.setLevel(logging.ERROR)

free_status = True
search_str = None
no_of_comments = 0


class new_thread:

    def __init__(self):
        global search_str, no_of_comments
        self.search_str = search_str
        self.no_of_comments = no_of_comments
        thread = threading.Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        global free_status
        scr = Scrapper_Class()
        scr.get_comments(self.search_str, self.no_of_comments)
        scr.close_driver()
        logs.log_info("completed_thread_run")
        free_status = True


@app.route('/', methods=['GET', 'POST'])
@cross_origin()
def home():
    pa = product_analytics()
    total_searches, most_searched, max_rev, max_rated, least_rated, max_discount = pa.product_stats()
    return render_template("index.html",
                           analytics={'total_searches': str(total_searches), 'most_searched': most_searched,
                                      'highest_reviewed': max_rev,
                                      'most_successful': max_rated, 'least_successful': least_rated,
                                      'max_discounted': max_discount})


@app.route('/comments', methods=['GET', 'POST'])
@cross_origin()
def comments():
    if request.method == 'POST':
        try:
            global free_status, search_str, no_of_comments
            if not free_status:
                return "The server is busy serving other request. Try after some time."
            search_str = request.form['search_str']
            no_of_comments = int(request.form['no_of_searches'])
            logs.log_info('Obtained the inputs from the user.')
            free_status = False
            new_thread()
            logs.log_info('initialised a new search process.')
            time.sleep(10)
            return redirect(url_for('wait_here'))
        except Exception as e:
            logs.log_error('Error in building the comments page.: ', str(e))
            raise Exception('Error in building the comments page.: \n' + str(e))
    else:
        return redirect(url_for("home"))


@app.route('/wait_here', methods=['GET', 'POST'])
@cross_origin()
def wait_here():
    if free_status:
        comment_data = pd.read_csv('comments.csv')
        product_data = pd.read_csv('searched_products.csv')
        comments_obt = comment_data.to_dict(orient='records')
        products_obt = product_data.to_dict(orient='records')
        logs.log_info('Building the results page .... ')
        return render_template("comments.html",
                               products=products_obt, reviews=comments_obt)
    else:
        return redirect(url_for('wait'))


@app.route('/wait', methods=['GET', 'POST'])
@cross_origin()
def wait():
    return render_template("wait.html")


@app.route('/download_searched_products', methods=['GET', 'POST'])
@cross_origin()
def download_searched_products():
    return send_file("searched_products.csv", as_attachment=True)


@app.route('/download_comments', methods=['GET', 'POST'])
@cross_origin()
def download_comments():
    return send_file("comments.csv", as_attachment=True)


@app.route('/download_img/<image_name>', methods=['GET', 'POST'])
@cross_origin()
def download_img(image_name):
    path = "plots/" + image_name + ".png"
    return send_file(path)


if __name__ == "__main__":
    app.debug = True
    app.run()

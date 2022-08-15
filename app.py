from flask import Flask, render_template, request, jsonify, Response, url_for, redirect, send_file
from flask_cors import CORS, cross_origin
import pandas as pd
from Flipkart_Scrapper import Scrapper_Class

from logger import my_logs

logs = my_logs('app.py')

app = Flask(__name__, template_folder="templates")


@app.route('/', methods=['GET', 'POST'])
@cross_origin()
def home():
    return render_template("index.html")


@app.route('/comments', methods=['GET', 'POST'])
@cross_origin()
def comments():
    if request.method == 'POST':
        try:
            search_str = request.form['search_str']
            no_of_comments = int(request.form['no_of_searches'])
            logs.log_info('Obtained the inputs from the user.')
            scr = Scrapper_Class()
            scr.get_comments(search_str, no_of_comments)
            scr.close_driver()
            comment_data = pd.read_csv('comments.csv')
            product_data = pd.read_csv('searched_products.csv')
            comments_obt = comment_data.to_dict(orient='records')
            products_obt = product_data.to_dict(orient='records')
            logs.log_info('Building the results page .... ')
            return render_template("comments.html", search_str=search_str, no_comments=no_of_comments,
                                   products=products_obt, reviews=comments_obt)
        except Exception as e:
            logs.log_error('Error in building the comments page.: ', str(e))
            raise Exception('Error in building the comments page.: \n' + str(e))
    else:
        return redirect(url_for("home"))


@app.route('/download_searched_products', methods=['GET', 'POST'])
@cross_origin()
def download_searched_products():
    return send_file("searched_products.csv", as_attachment=True)


@app.route('/download_comments', methods=['GET', 'POST'])
@cross_origin()
def download_comments():
    return send_file("comments.csv", as_attachment=True)


if __name__ == '__main__':
    app.run()

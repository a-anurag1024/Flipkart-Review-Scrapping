from flask import Flask, render_template, request, jsonify, Response, url_for, redirect
from flask_cors import CORS, cross_origin
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
            no_of_comments = request.form['no_of_searches']
            scr = Scrapper_Class()
            dfs = scr.get_comments(search_str, no_of_comments)
            df = scr.add_dfs(dfs)                                   # # # # # NEED TO DEFINE THIS FUNCTION
            comments_obt = df.to_json(orient='records')
            return render_template("comments.html", search_str=search_str, no_comments=no_of_comments, reviews=comments_obt)
        except Exception as e:
            logs.log_error('Error in building the comments page.: ', str(e))
            raise Exception('Error in building the comments page.: \n'+str(e))
    else:
        return redirect(url_for("home"))


if __name__ == '__main__':
    app.run()


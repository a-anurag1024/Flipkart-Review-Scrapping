# Flipkart-Review-Scrapping
This project is related to scrapping of product and customer review data from the Flipkart website. It also tries to perform some basic analytics on the gathered data.

### Deployed Link:-
[git-flipkart-scrapper.herokuapp.com](https://git-flipkart-scrapper.herokuapp.com)

## Tech used:-
- Python
- Selenium (for web-scrapping)
- Flask API
- Cassandra DB (for database)
- Heroku (for deployment)

## Basic functionalities:-
- Scraps the customer comments and reviews for the specified product name and number of comments. The following information on the product review are collected:- comment title, comment body, rating, commentend when, location of the commenter, number of comment likes and dislikes
- The app also collects information on the product details whose customer comments were made available. The following information on the product details are collected:- Product category, Product name, Product MRP, Product price (at the time of scrapping), overall rating, number of reviews, number of ratings, number of ratings in each individual rate [5,4,3,2,1]
- On the analytics side, basic analytics are performed to grow insights on the product categories that have been searched so far using the app. Stats like total number of searches, total number of ratings, average ratings, average discount per category explored till now are calculated and shown.
- To check the amount of black and white thinking in the scrapped customer reviews, a metric is introduced and (see below) is calculated for the different categories.

## Amount of black and white thinking:-
It is usually observed that the amongst the five rating levels, the most selected rating levels are usually the 5 star or 1 star. This might be linked to human tendency to have a black and white (either the product is good or bad) view on the product. The ratings in between (2,3 and 4 stars) reflects nuance in the process of reviewing. Another thing to note on this is that while the large number of one and five star ratings may indicate to lack of nuance, but it might very well also indicate to product being actually absolutely disliked/liked by the customers.

## High level system design:-
- **app.py** :- Builds the flask API web-app
- **database.py** :- establishes the connection with the cassandra database
- **Flipkart_Scrapper.py** :- establishes the web driver, scraps the required data from the Flipkart website and stores the data in the database
- **page_locators.py** :- locates the various page elements in the flipkart website
- **product_analysis.py** :- performs the basic product analysis

We can check the trends on the amount of black and white thinking vs nuance thinking in the customers by making a rudimentry measure that roughly acknowledges the factors mentioned above. We can define the measure as:-

Amount of black-white thinking = [1/mod(n5-n1)]\*[1/mod(n2+n3+n4)]\*[total_ratings^2]

## Usage:-
- One can interact with the app directly using the deployed link given above. Note that while using the deployed link one may face server overload as this app is hosted freely at heroku. In such a case, please wait for a while and then enter your enquiry again. Also, note that scrapping is a time taking process. Please have a look at the benchmarks to know about the approximate time required to complete a scrapping request.
- If someone wants to use the app in developer's mode, one can do so by directly cloning this repo. Note that in that case, you have to use your own cassandra database. To do so, please update the **cassandra_credentials.txt** according to your cassandra db. You would also need to install the dependencies using the command 
```
pip install -r requirements.txt
```
## Benchmarks:-
Please note that it takes a lot of time in the backend to scrap the web for the products and comments as the webdriver emulates the visitation of each of the product and comments page. So, it is requested to wait for some time (2-3 minutes) before the result page is shown. Although the wait time is highly dependent on the latency between the Heruko server (where this web app is hosted) and the Flipkart webpage, you can have a look at the benchmarks given below:-
- product searched:- **stopwatch** || number of comments:- **10** || wait-time:- **2m:21sec**
- product searched:- **Macbook pro** || number of searches:- **5** || wait-time:- **1m:47sec**
- product searched:- **Bose headphones** || number of searches:- **3** || wait-time:- **0m:55sec**

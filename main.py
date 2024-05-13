import pymongo
import pandas as pd
import requests
from io import StringIO
import yfinance as yf
import asyncio
from twscrape import API
from datetime import datetime, timezone, timedelta
from openai import OpenAI
import re
import random
import matplotlib.pyplot as plt
import matplotlib.dates as mdates



def import_weights():
    """
    Scrape the SP100 stock weights from blackrock website.
    Composition is saved in a dataframe df that contains ticker, weights and market value
    """

    response = requests.get("https://www.blackrock.com/us/individual/products/239723/ishares-sp-100-etf/1464253357814.ajax?fileType=csv&fileName=OEF_holdings&dataType=fund", timeout = 10)
    if response.status_code == 200:
        # Read response as a file
        df = pd.read_csv(StringIO(response.text), skiprows=9)
        df = df[df['Asset Class'] == 'Equity'] #filtering only equity 
        
        if df is not None:
    
            weights = df[['Ticker', 'Weight (%)','Market Value']].dropna()
            print(f"Successfully imported the weights of {len(weights)} companies")
            #Create a dictionary with ticker, weights and Market value of only the 75%. It Removes blank lines
            
            weights['Cumulative Weight'] = weights['Weight (%)'].cumsum()
            weights_75 = weights[weights['Cumulative Weight'] <= 75]
            print(f"Successfully filtered the top 75%, with a total of {len(weights_75)} companies")
            return weights_75
    
    else:
        print(f"Failed to download file: status code {response.status_code}")
        return None


def daily_prices(df, start_date, end_date):
    """
    Scrape the stocks' daily prices contained in the df for a specific date range
    Ticker BRKB is called BRK-B on yahoo finance
    Daily prices are saved in a dataframe price_df
    """
    prices = {}         #create a dictionary with tickets as keys
    for ticker in df["Ticker"]:
        if ticker == "BRKB":                #replacing Berkshire's ticker for yfinance comaptibility
            data = yf.download("BRK-B", start=start_date, end=end_date)
        else:
            data = yf.download(ticker, start=start_date, end=end_date)

        prices[ticker] = data["Adj Close"]      #add prices series to dictionary
        print(f"Successfully downloaded {ticker} price timeseries")

    price_df = pd.DataFrame(prices)             #convert dictionary to dataframe
    return price_df


async def main(df,start_date, end_date):
    """
    Scrape the tweets for every ticker in the desired date range using the twscrape library
    Tweets are saved in a pymongo database, where we save the tweet id, content, date and ticker
    """
    
    api = API()  # or API("path-to.db") - default is `accounts.db`
    
    # Convert string values to datetime objects
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)

    await api.pool.add_account("username twitter", "password twitter", "mail twitter", "password email")

    
    await api.pool.login_all()
    
    db = client["tweets_database"]  # database name
   
    for ticker in df["Ticker"]:
        print(f"Starting download of ${ticker} tweets")
        q = f"${ticker} since:{start_date} until:{end_date}"

        collection_name = f"tweets_{ticker}"  
        collection = db[collection_name]
        
        async for tweet in api.search(q):
            print(tweet.id, tweet.date)
            
            # Check if the tweet date is within the desired range
            if start_date_dt <= tweet.date <= end_date_dt:
                print(tweet.id, tweet.date)
                criteria = {"tweet_id": tweet.id}
                # Store the tweet in MongoDB
                tweet_data = {
                    "ticker": ticker,
                    "tweet_id": tweet.id,
                    "date": tweet.date,
                    "content": tweet.rawContent
                }
                collection.update_one(criteria,{"$set": tweet_data}, upsert=True) 

        print(f"Finished downloading ${ticker} tweets")


def analyze_tweet_sentiment(tweet,api_key):
    """
    Analyze the sentiment of a tweet using the OpenAI ChatGPT API.
    Returns 'bullish', 'bearish', or 'unknown'.
    """
    client = OpenAI(api_key = api_key)

    # prompt for chatgpt
    prompt= [
        {"role":"user","content":f"Comment the following tweet's sentiment saying only bullish or bearish\n'{tweet}'"}
    ]

    try:
        response =  client.chat.completions.create(
            model="gpt-3.5-turbo",  #Most economic
            messages=prompt,
            max_tokens=5  # Limiting tokens to minimize cost
        )

        # Extracting and processing the response text
        response_text = response.choices[0].message.content.strip().lower()

        if "bullish" in response_text:
            return "bullish"
        elif "bearish" in response_text:
            return "bearish"
        else:
            return "unknown"  # In case the response is unclear

    except Exception as e:
        print(f"An error occurred: {e}")
        return "unknown"  # Error handling

def get_cashtag_sentiments(df,sample_size_requested,api_key):
    """
    Access the pymongo database to iterate for every ticker the tweets, which are then analyzed with the function analyze_tweet_sentiment(tweet)
    A dictionary (cashtag_sentiments) is created where every ticker is a key, and the value is a list (sentiments) containing all tweets's sentiment.
    """
    db = client["tweets_database"]
    cashtag_sentiments = {}
    cashtag_pattern = re.compile(r'\$[A-Za-z]{1,5}')    #Pattern to search cashtag, $ followed by 1-5 letters

    for ticker in df["Ticker"]:
        print(f"Starting to evaluate sentiment for ${ticker} tweets")
        collection_name = f"tweets_{ticker}"
        collection = db[collection_name]

        sentiments = []
        list_tweets = list(collection.find())
        sample_size = 0
        
        while sample_size < sample_size_requested:
            if len(list_tweets)>=1:
                #Exctracting a random tweet and removing it from the list then.
                random_tweet = random.randrange(len(list_tweets))
                tweet = list_tweets[random_tweet]
                list_tweets.pop(random_tweet)

                # Find all cashtags in the tweet
                cashtags_found = cashtag_pattern.findall(tweet['content'])

                ''' Since we already know that the tweet contains the cashtag of the current ticker, 
                    if it contains any additional cashtags (more than one in total), we cannot be certain 
                    which stock the tweet is referring to. Therefore, such tweets are skipped and not analyzed
                '''
                if len(set(cashtags_found)) > 1:
                    continue

                sentiment = analyze_tweet_sentiment(tweet['content'],api_key)
                print(f"${ticker} tweet sentiment analysis: {sentiment}")
                
                if sentiment in ["bullish", "bearish"]:
                    sentiments.append(sentiment)
                    sample_size +=1
            
            else:
                break

        cashtag_sentiments[ticker] = sentiments
        print(f"Finished to evaluate sentiment for ${ticker} tweets")

    return cashtag_sentiments

def count_occurrences_and_save_total(df, start_date, end_date):
    '''Function to count for each day the total amount of tweets for every ticker
    amounts are saved in the same database'''
    db = client["tweets_database"]
 
    date_counts = {}
 
    totalcount_collection_name = f"totalcount"  
    totalcount_collection = db[totalcount_collection_name] 
    totalcount_collection.drop()
    
    daycount_total_collection_name = f"daycount_total"
    daycount_total_collection = db[daycount_total_collection_name]
    daycount_total_collection.drop()

    formatted_start_date = datetime.strptime(start_date, "%Y-%m-%d")
    formatted_end_date = datetime.strptime(end_date, "%Y-%m-%d")
    for ticker in df["Ticker"]: 
        ticker_collection_name = f"tweets_{ticker}"  
        ticker_collection = db[ticker_collection_name]
        for record in ticker_collection.find({"date": {"$gte": formatted_start_date, "$lte": formatted_end_date}}): 
                date = record["date"].date()  # Extracting the date part of the datetime
                daycount_collection_name = f"daycount_{ticker}"
                daycount_collection = db[daycount_collection_name]
                daycount_collection.drop()

                date_counts[date] = date_counts.get(date, 0) + 1

        
        for date, count in date_counts.items():
            formatted_date = date.strftime("%Y-%m-%d")
            daycount_collection_name = f"daycount_{ticker}"
            daycount_collection = db[daycount_collection_name]

            daycount_data = {
                "ticker": ticker,
                "date": formatted_date,
                "tweet_count": count
            }

            daycount_total_collection.update_one({"date": formatted_date},{'$inc': {'tweet_count': count}}, upsert=True)
            # Insert or update the document in the daycount collection
            daycount_collection.update_one({"ticker": ticker, "date": formatted_date}, {"$set": daycount_data}, upsert=True)

        # Calculate and save the total tweet count for the specific ticker
        total_tweet_count = ticker_collection.count_documents({"date": {"$gte": formatted_start_date, "$lte": formatted_end_date}})
        print("total: "+ ticker+ "="+ str(total_tweet_count))
        criteriatotal = {"ticker": ticker}

        totalcount_data = {
            "ticker": ticker,
            "total_tweet_count": total_tweet_count
        }
    
        totalcount_collection.update_one(criteriatotal, {"$set": totalcount_data}, upsert=True)
 
def portfolios_by_tweets(df):
    '''Creates two stock list, one overtweeted and one undertweed.
        If the tweets weight of a stock are more than the weights adjusted, the stock is considered overtweeted
        Otherwise undertweeted
    '''
    db = client["tweets_database"]  # database name
    totalcount_collection_name = f"totalcount"  
    totalcount_collection = db[totalcount_collection_name] 
    total_tweet_counts = totalcount_collection.distinct("total_tweet_count")

    total_index_tweet_count = round(sum(total_tweet_counts))
    print(total_index_tweet_count)
    overtweeted = []
    undertweeted = []
    for record in totalcount_collection.find():
        ticker = record["ticker"]
        total_tweet_count = record["total_tweet_count"]
        tweet_weight= total_tweet_count/total_index_tweet_count
        weight = df[df['Ticker'] == ticker]['Weight (%)'].values[0]
        if tweet_weight >(weight/100) * (100/75):
            overtweeted.append(ticker)
        else:
            undertweeted.append(ticker)
    return overtweeted,undertweeted

def portfolios_by_sentiment(cashtag_sentiments):
    '''Uses the dictionary cashtag_sentiments to count which sentiment prevails
        If more than the 50% of tweets are bullish for a ticker, the stock is put in the portfolio bullish_list
        otherwise the stock is put in the portfolio bearish_list
    '''
    bullish_list = []
    bearish_list = []

    for ticker, sentiments in cashtag_sentiments.items():
        bullish_count = sentiments.count("bullish")
        bearish_count = sentiments.count("bearish")
        print(f"${ticker} tweets are divided in {str(bullish_count)} bullish and {str(bearish_count)} bearish")
        if bullish_count > bearish_count:
            bullish_list.append(ticker)
        elif bearish_count > bullish_count:
            bearish_list.append(ticker)
        
    
    return bullish_list, bearish_list

def calculate_portfolio_return(stock_list, price_df):
    '''Uses the bearish and bullish lists to calculate the total return of the two portfolios
        Total return is calculated by doing an average of the total returns of all the stocks in the list.
        Total return of the single stock is calculated by using the adj. close price at the beginning and ending of the price_df 
    '''
    total_return = 0
    num_stocks = len(stock_list)

    if num_stocks == 0:
        return 0

    for ticker in stock_list:
        # Ensure ticker is in the DataFrame
        if ticker in price_df.columns:
            start_price = price_df[ticker].iloc[0]  # First (start) price
            end_price = price_df[ticker].iloc[-1]   # Last (end) price

            # Calculate the return for this stock
            stock_return = ((end_price - start_price) / start_price)
            total_return += stock_return

    # Average return across all stocks in the list
    average_return = round((total_return / num_stocks)*100, 4)
    return average_return

def timeseries_stock(stock_choice, api_key, start_date, end_date):
    '''Provide a timeseries analysis of a chosen stock's sentiment.
    The function creates a dictionary where to store the amount of bullish and bearish tweets for every day
    Then iterates for every tweet of the stock, and calling analyze_tweet_sentiment() store the sentiment analysis value inside the dictionary.
    '''
    db = client["tweets_database"]
    collection_name = f"tweets_{stock_choice}"
    collection = db[collection_name]
    cashtag_pattern = re.compile(r'\$[A-Za-z]{1,5}')

    '''Creating a dictionary with days inside the range date as keys, 
    and lists [0,0] as values of the keys where to store the amount of bullish and bearish tweets'''
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    date_dict = {}
    current_date = start
    while current_date <= end:
        date_dict[current_date.strftime("%Y-%m-%d")] = [0, 0]
        current_date += timedelta(days=1)
    
    '''Iterate for every tweet of the stock and store the result in the dictionary
        Again, tweets with multiple cashtag are skipped'''
    for tweet in collection.find():
        cashtags_found = cashtag_pattern.findall(tweet['content'])
        if len(set(cashtags_found)) > 1:
            continue

        sentiment = analyze_tweet_sentiment(tweet['content'],api_key)
        print(f"${stock_choice} tweet sentiment analysis: {sentiment}")
        
        if sentiment in ["bullish", "bearish"]:
            tweet_date = tweet["date"].strftime('%Y-%m-%d')
            if sentiment == "bullish":
                date_dict[tweet_date][0] += 1
            else:
                date_dict[tweet_date][1] += 1 
    
    return date_dict



def plot_total_tweet_per_day():
    '''Function to plot the daily amount of tweet for each day in the range'''
    db = client["tweets_database"]

    daycount_total_collection_name = f"daycount_total"
    daycount_total_collection = db[daycount_total_collection_name]
    data = list(daycount_total_collection.find({}, {'_id': 0, 'date': 1, 'tweet_count': 1}))

    # Convert to pandas DataFrame for easier plotting
    df = pd.DataFrame(data)
    # Ensure 'date' is in datetime format
    # Sort the DataFrame by the 'date' column to ensure chronological order
    df = df.sort_values(by='date')

    # Plotting the sorted data
  # Plotting without converting date strings to datetime objects
    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['tweet_count'], marker='o')  # marker is optional

    # Set the x-axis labels to the date strings as they are
    plt.xticks(rotation=90)  # Rotate the x-axis labels for better readability

    plt.title('Tweet Count Over Time')
    plt.xlabel('Date')
    plt.ylabel('Tweet Count')
    plt.grid(True)
    plt.tight_layout()  # Adjust the layout to make room for the x-axis labels
    plt.show()


def plot_return_list(stock_list, price_df, start_date, end_date):
    '''Function to plot the daily return from a list'''
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    current_date = start
    range_date_list = []
    while current_date < end:
        range_date_list.append(current_date)
        current_date += timedelta(days=1)
    

    daily_performances=[]

    for c, i in enumerate(range_date_list):
        if c!=0:
            tot_daily_return = 0
            skip = "no"
            for x in stock_list:
                try: 
                    stock_daily_return =(price_df.loc[i,x]/price_df[x].shift(1).loc[i]) -1
                    tot_daily_return += stock_daily_return #return of stock x on day i
                except:
                    skip = "yes"
                    break
            if skip == "no":
                tot_daily_return/= len(stock_list)
            daily_performances.append({"date":i,"return":tot_daily_return})

    # Convert to pandas DataFrame for easier plotting
    df = pd.DataFrame(daily_performances)
   
    # Sort the DataFrame by the 'date' column to ensure chronological order
    df = df.sort_values(by='date')

    # Plotting the sorted data
    # Plotting without converting date strings to datetime objects
    plt.figure(figsize=(10, 6))
    plt.plot(df['date'], df['return'], marker='o')  # marker is optional

        
    # Use DateFormatter to format the x-axis labels
    ax = plt.gca()
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d'))
    ax.xaxis.set_major_locator(mdates.DayLocator(interval=1))  # Show every day

    # Rotate and align the x-axis labels
    plt.setp(ax.get_xticklabels(), rotation=90, ha='right')

    plt.title('Portfolio Performance')
    plt.xlabel('Date')
    plt.ylabel('Return')
    plt.grid(True)
    plt.tight_layout()  # Adjust the layout to make room for the x-axis labels
    plt.show()

def plot_timerseries(dictionary,stock_choice):
    '''Function to plot daily proportion between Bullish and total tweets for stock chosen'''
    
    # Prepare lists for plotting
    dates = list(dictionary.keys())
    percentages_corrected  = [v[0] / (v[0] + v[1]) * 100 for v in dictionary.values()]

    # Plotting
    plt.figure(figsize=(10, 6))
    plt.plot(dates, percentages_corrected, marker='o')

    # Formatting the plot
    plt.title(stock_choice+' Daily Bullish/Total proportion')
    plt.xlabel('Date')
    plt.ylabel('Percentage (%)')
    plt.xticks(rotation=90)  
    plt.tight_layout()  # Adjust layout
    plt.grid(True)
    plt.show()








#inititating parameter 


start_date = "2023-12-01"
end_date = "2024-01-11"

sample_size_requested = 1000
api_key = 'insert_api_key'
client = pymongo.MongoClient("mongodb://localhost:27017/") 

stock_choice = "TMO"

#SCRAPING PART
weights_75 = import_weights()

price_df = daily_prices(weights_75,start_date,end_date)

if __name__ == "__main__":
    asyncio.run(main(weights_75,start_date,end_date))

#TIMESERIES FOR TWEET COUNT
count_occurrences_and_save_total(weights_75, start_date, end_date)


#OVER UNDER TWEETED PART EXECUTION

overtweeted_list,undertweeted_list = portfolios_by_tweets(weights_75)

return_overtweeted_portfolio = calculate_portfolio_return(overtweeted_list, price_df)
print(f"\nOvertweeted portfolio is composed by the following stocks:\n{overtweeted_list} \nThat has a return of: {str(return_overtweeted_portfolio)}% between {start_date} and {end_date}\n")

return_undertweeted_portfolio = calculate_portfolio_return(undertweeted_list, price_df)
print(f"\nUndertweeted portfolio is composed by the following stocks:\n{undertweeted_list} \nThat has a return of: {str(return_undertweeted_portfolio)}% between {start_date} and {end_date}\n")


#SENTIMENT PART EXECUTION

cashtag_sentiments = get_cashtag_sentiments(weights_75,sample_size_requested,api_key)

bullish_list, bearish_list = portfolios_by_sentiment(cashtag_sentiments)

return_bullish_portfolio = calculate_portfolio_return(bullish_list, price_df)
print(f"\nBullish portfolio is composed by the following stocks:\n{bullish_list} \nThat has a return of: {str(return_bullish_portfolio)}% between {start_date} and {end_date}\n")

return_bearish_portfolio = calculate_portfolio_return(bearish_list, price_df)
print(f"\nBearish portfolio is composed by the following stocks:\n{bearish_list} \nThat has a return of: {str(return_bearish_portfolio)}% between {start_date} and {end_date}\n")



#PLOTTING PART
plot_total_tweet_per_day()


#HERE WE PLOT THE DAILY RETURN OF THE PORTFOLIOS CREATED
plot_return_list(overtweeted_list, price_df, start_date, end_date)
plot_return_list(undertweeted_list, price_df, start_date, end_date)
plot_return_list(bullish_list, price_df, start_date, end_date)
plot_return_list(bearish_list, price_df, start_date, end_date)


#TIMESERIES

date_dict = timeseries_stock(stock_choice, api_key, start_date, end_date)

plot_timerseries(date_dict, stock_choice)

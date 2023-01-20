from fastapi import FastAPI
import pandas as pd


app = FastAPI()

# Number of times a keyword appears in movies/series Title's, by platform
# 1.- get_word_count('netflix', 'love')
@app.get("/word_count/{platform}/{word}")
async def read_item(platform: str, word: str):
    if platform in ['netflix', 'hulu', 'disney', 'amazon']:
        df = pd.read_csv(platform+'.csv')
        count = df['title'].str.count(word).sum()
        response = dict(platform=platform, cantidad=str(count))
        return response
    else:
        # return {'platform not available': platform}
        return {f'Platform not available: {platform}. Try again.'}


# Number of films by platform with a score greater than XX in a given year
# 2.- get_score_count('netflix', 85, 2010)
@app.get("/score_count/{platform}/{score}/{year}")
async def read_items(platform: str, score: int, year: int):
    if platform in ['netflix', 'hulu', 'disney', 'amazon']:
        df = pd.read_csv(platform+'.csv')
        number = df[(df['score'] > score) & (df['release_year'] == year)]
        number = len(number)
        response = dict(platform=platform, cantidad=str(number))
        return response
    else:
        return {f'Platform not available: {platform}. Try again.'}

# TODO 
# The second highest scoring film for a given platform, based on title's alphabetical order.
# 3.- get_second_score('amazon')
@app.get("/second_score/{platform}")
async def read_item(platform: str):
    if platform in ['netflix', 'hulu', 'disney', 'amazon']:
        df = pd.read_csv(platform+'.csv')
        second = df.sort_values(by=['score', 'title'], ascending=[False, True]).iloc[1]['title']
        score = df.sort_values(by=['score', 'title'], ascending=[False, True]).iloc[1]['score']       
        response = dict(title=second, score=str(score))
        return response


# Longest films according to year, platform and duration type
# 4.- get_longest('netflix', 'min', 2016)
@app.get("/longest/{platform}/{duration_type}/{year}")
async def read_items(platform: str, duration_type: str, year: int):
    if platform in ['netflix', 'hulu', 'disney', 'amazon']:
        df = pd.read_csv(platform+'.csv')
        longest = df[(df['duration_type'] == duration_type) & (df['release_year'] == year)].sort_values(by=['duration_int'], ascending=False)[['title', 'duration_int','duration_type']].head(1)
        response = longest.to_dict()
        return response
    else:
        return {f'Platform not available: {platform}. Try again.'}


# Number of series and movies by rating
# 5.- get_rating_count('18+')
@app.get("/rating_count/{rating}")
async def read_item(rating: str):
    df = pd.read_csv('df_full.csv')
    count = (df['rating'] == '18+').sum()
    response = dict(rating=rating, cantidad=str(count))
    return response

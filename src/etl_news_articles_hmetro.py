import pandas as pd
import requests
from bs4 import BeautifulSoup
import html
import json
import re


def get_body(url):
    article_url = f"https://www.hmetro.com.my/{url}"
    response = requests.get(article_url)
    soup = BeautifulSoup(response.text, "html.parser")
    body = soup.find_all("article-component")
    string = re.search(r"\{.*\}", str(body)).group(0)
    string_parsed_html = html.unescape(string)
    json_string = json.loads(string_parsed_html)
    return json_string["body"]


def extract_webpages():
    lst_webpages = []
    for i in range(10):
        response = requests.get(f"https://www.hmetro.com.my/api/topics/169?page={i}")
        data = response.json()
        lst_webpages.extend(data)
    return lst_webpages


def get_dataset(lst_webpages: list) -> pd.DataFrame:
    dataset = []
    for index, item in enumerate(lst_webpages):

        observations = {}
        observations["title"] = item["title"]
        observations["url"] = item["url"]
        try:
            observations["author_name"] = item["field_article_author"]["name"]
        except:
            observations["author_name"] = None
        observations["word_count"] = item["word_count"]

        dataset.append(observations)
    df = pd.DataFrame(dataset)
    return df


def transform_pipeline(df):
    df["body"] = df["url"].apply(get_body)
    df = df.assign(
        body=df.body.str.replace(
            r"<\/?[a-zA-Z][^>]*>|(©.*Bhd)|(^[\w\d@.]*.my)", "", regex=True
        ).str.strip()
    )
    return df


def load_data(df):
    df.to_csv("data/hmetro_news_articles.csv", index=False)


def main():
    webpages = extract_webpages()
    df = get_dataset(webpages)
    df = transform_pipeline(df)
    load_data(df)


if __name__ == "__main__":
    main()

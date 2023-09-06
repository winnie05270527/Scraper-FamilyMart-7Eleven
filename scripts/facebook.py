from sqlalchemy import create_engine
from facebook_scraper import get_posts
from time import sleep
from random import randint
import config
import pandas as pd
import os
import json

def crawl_facebook_posts(fanpage, page_default, cookies, save_path):
    i = 1
    data = []
    for post in get_posts(fanpage, pages=page_default, cookies=cookies, options={"reactors": True}):
        try:
            post_data = {
                'user_id': str(post['user_id']),
                'username': str(post['username']),
                'time': post['time'].strftime("%Y-%m-%d %H:%M:%S"),
                'post_url': post['post_url'],
                'post_id': str(post['post_id']),
                'post_text': post['post_text'].strip().replace("\n", ""),
                'like_count': post.get('reactions', {}).get('讚', 0),
                'love_count': post.get('reactions', {}).get('大心', 0),
                'go_count': post.get('reactions', {}).get('加油', 0),
                'wow_count': post.get('reactions', {}).get('哇', 0),
                'haha_count': post.get('reactions', {}).get('哈', 0),
                'sad_count': post.get('reactions', {}).get('嗚', 0),
                'angry_count': post.get('reactions', {}).get('怒', 0),
                'share_count': post.get('comments', 0),
                'comment_count': post.get('shares', 0),
            }
            with open(save_path, "a", encoding='utf-8') as file:
                file.write(json.dumps(post_data, ensure_ascii=False) + "\n")
            data.append(post_data)
        except Exception as e:
            print(e)
        i += 1
        sleep(randint(10, 60))
    df = pd.DataFrame(data)
    return df

if __name__ == "__main__":
    DB_HOST = config.DB_HOST
    DB_USER = config.DB_USER
    DB_PASSWORD = config.DB_PASSWORD
    DB_DATABASE = config.DB_DATABASE
    
    project_directory = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    db_url = f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}'
    engine = create_engine(db_url)

    for fanpage in ["711open", "FamilyMart"]:
        pages = 50
        cookies_path = os.path.join(project_directory, 'data', 'www.facebook.com_cookies.txt')
        save_path = os.path.join(project_directory, 'fb_posts.jsonl')
        df = crawl_facebook_posts(fanpage, pages, cookies_path, save_path)
        df.to_sql(name='fb_posts', con=engine, if_exists='append', index=False)

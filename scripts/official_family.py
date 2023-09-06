from sqlalchemy import create_engine, Column, String, Text, Integer, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import config
from datetime import datetime
import requests
from bs4 import BeautifulSoup


Base = declarative_base()

class FamilyEvent(Base):
    __tablename__ = 'official_family'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255))
    period = Column(String(50))
    content = Column(Text)
    label = Column(Text)
    month = Column(String(4)) # 2023/06 -> 2306
    
    __table_args__ = (UniqueConstraint('month', 'title', name='_month_title_uc'),)
    
def get_family_events():
    url = 'https://www.family.com.tw/Marketing/Event#all'
    headers = {
        'User-Agent': 'Your-User-Agent-Here',
    }
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')
    tab = soup.find(id="all")
    cards = tab.findAll(class_="card card--event")
    events = []
    for card in cards:
        title = card.find(class_="card__title").text.strip() or "無"
        period = card.find(class_="card__date").text.strip() or "無"
        content = card.find(class_="card__text line-clamp").text.strip() or "無"
        label = card.find(class_="card__label").text.strip() or "無"

        event = FamilyEvent(
            title=title,
            period=period,
            content=content,
            label=label
        )
        events.append(event)
    return events


if __name__ == "__main__":
    DB_HOST = config.DB_HOST
    DB_USER = config.DB_USER
    DB_PASSWORD = config.DB_PASSWORD
    DB_DATABASE = config.DB_DATABASE

    engine = create_engine(f'mysql+pymysql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_DATABASE}?charset=utf8mb4')
    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()

    events = get_family_events()

    for event in events:
        current_month = datetime.now().strftime('%y%m')
        event.month = current_month
        session.merge(event)

    session.commit()
    session.close()
from sqlalchemy import create_engine, Column, String, Text, Integer, UniqueConstraint
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
import config
from datetime import datetime
import requests
import xml.etree.ElementTree as ET
import re

Base = declarative_base()

class SevenEvent(Base):
    __tablename__ = 'official_seven'
    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(255))
    period = Column(String(50))
    content = Column(Text)
    remark = Column(Text)
    month = Column(String(4)) # 2023/06 -> 2306
    
    __table_args__ = (UniqueConstraint('month', 'title', name='_month_title_uc'),)

def get_711_events():
    url = 'https://www.7-11.com.tw/readxml.aspx'
    headers = {
        'User-Agent': 'Your-User-Agent-Here',
    }
    payload = {
        'num': '0'
    }
    response = requests.post(url, headers=headers, data=payload)
    xml_data = response.content
    bd = ET.fromstring(xml_data)
    items = bd.iter('Item')
    events = []
    for item in items:
        title = item.find('APP_BannerTitle').text or "無"
        period = item.find('Period').text or "無"
        content = item.find('Content').text or "無"
        remark = item.find('Remark').text or "無"

        pattern = r"<font.*?>|<\/font>"
        remark = re.sub(pattern, "", remark)

        event = SevenEvent(
            title=title,
            period=period,
            content=content,
            remark=remark
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

    events = get_711_events()

    for event in events:
        current_month = datetime.now().strftime('%y%m')
        event.month = current_month
        session.merge(event)

    session.commit()
    session.close()
from sqlalchemy import Column, Integer, String, Float, DateTime, Text, ForeignKey, create_engine
from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy.dialects.postgresql import JSON, ARRAY

Base = declarative_base()

class Ticker(Base):
    __tablename__ = "ticker"
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, unique=True, nullable=False, index=True)
    cik = Column(String, unique=True, nullable=False, index=True)

    icon = Column(String, nullable=True)
    display_name = Column(String, nullable=True)
    industry = Column(String, nullable=True)

    last_updated = Column(DateTime(timezone=True), nullable=True)

    recommendation_trends = Column(ARRAY(JSON), nullable=True)
    earnings_surprises = Column(ARRAY(JSON), nullable=True)
    insider_sentiment = Column(ARRAY(JSON), nullable=True)

    last_updated_news = Column(DateTime(timezone=True), nullable=True)

    balance_sheet = Column(JSON, nullable=True)
    income_statement = Column(JSON, nullable=True)
    cash_flow = Column(JSON, nullable=True)

    # articles = relationship(
    #     "Article",
    #     # secondary=ticker_article,
    #     back_populates="tickers",
    #     lazy="selectin"
    # )

    company_events = relationship("CompanyEvent", back_populates="ticker")

    insider_trades = relationship("InsiderTrade", back_populates="ticker")

    def __json__(self):
        return {
            "id": self.id,
            "ticker": self.ticker,
            "cik": self.cik,
            "icon": self.icon,
            "display_name": self.display_name,
            "industry": self.industry,

            "last_updated": self.last_updated,
            "recommendation_trends": self.recommendation_trends,
            "earnings_surprises": self.earnings_surprises,
            "insider_sentiment": self.insider_sentiment,
            "last_updated_news": self.last_updated_news,
            
            # "articles": [
            #     article.__json__()
            # for article in self.articles
            # ]
        }


class CompanyEvent(Base):
    __tablename__ = "company_event"
    id = Column(Integer, primary_key=True, autoincrement=True)
    items = Column(JSON, nullable=True)
    summary = Column(Text, nullable=True)
    date = Column(String, nullable=True)

    cik = Column(String, ForeignKey("ticker.cik"), nullable=True)
    
    ticker = relationship("Ticker", back_populates="company_events")

    def __json__(self):
        return {
            "id": self.id,
            "date": self.date,
            "cik": self.cik,
            "items": self.items,
            "summary": self.summary,
        }

class InsiderTrade(Base):
    __tablename__ = "insider_trade"
    id = Column(Integer, primary_key=True, autoincrement=True)

    issuer = Column(String, nullable=True)
    insider = Column(String, nullable=True)
    position = Column(String, nullable=True)
    date = Column(String, nullable=True, primary_key=True) 
    
    transaction_type = Column(String, nullable=True) 
    code = Column(String, nullable=True) 
    description = Column(String, nullable=True)
    shares = Column(Float, nullable=True)
    price = Column(Float, nullable=True)
    value = Column(Float, nullable=True)
    remaining_shares = Column(Float, nullable=True)

    cik = Column(String, ForeignKey("ticker.cik"), nullable=True, primary_key=True)
    ticker = relationship("Ticker", back_populates="insider_trades")

    def __json__(self):
        return {
            "id": self.id,
            "cik": self.cik,
            "date": self.date,
            "issuer": self.issuer,
            "insider": self.insider,
            "position": self.position,
            "transaction_type": self.transaction_type,
            "code": self.code,
            "description": self.description,
            "shares": self.shares,
            "price": self.price,
            "value": self.value,
            "remaining_shares": self.remaining_shares
        }
from sqlalchemy import Column, Integer, String, Float, Date, DateTime, create_engine
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class InsiderTrade(Base):
    """
    Represents a single insider trade transaction.
    One Form 4 filing may result in multiple rows in this table.
    """
    __tablename__ = 'insider_trades'

    id = Column(Integer, primary_key=True, autoincrement=True)
    
    # Metadata
    ticker = Column(String, index=True)
    issuer = Column(String)
    form_type = Column(String)  # e.g., "Form 4"
    
    # Filing Date (when the form was filed)
    # Note: 'Date' in the JSON is usually the transaction date
    transaction_date = Column(Date)
    
    # Insider Details
    insider_name = Column(String)
    position = Column(String)
    
    # Transaction Details
    transaction_type = Column(String) # e.g., "Sale", "Purchase"
    transaction_code = Column(String) # e.g., "S", "P"
    description = Column(String)      # e.g., "Open Market Sale"
    
    # Financials
    # Storing as float, but Decimal represents money better if precision is critical
    shares = Column(Float)            
    price_per_share = Column(Float, nullable=True) 
    total_value = Column(Float, nullable=True)
    shares_owned_following = Column(Float, nullable=True)
    
    def __repr__(self):
        return f"<InsiderTrade(ticker='{self.ticker}', date='{self.transaction_date}', type='{self.transaction_type}', shares={self.shares})>"

# Example of how you would consume the JSON data:
# 
# json_data = {
#     'Transaction Type': {0: 'Sale', 1: 'Sale'}, 
#     'Shares': {0: 1601, 1: 3071}, 
#     ... 
# }
#
# Since the JSON is "column-oriented" (dict of dicts), you need to iterate by index:
#
# num_rows = len(json_data['Shares'])
# trades = []
# for i in range(num_rows):
#     trade = InsiderTrade(
#         ticker=json_data['Ticker'][i],
#         transaction_date=pd.to_datetime(json_data['Date'][i]).date(),
#         insider_name=json_data['Insider'][i],
#         transaction_type=json_data['Transaction Type'][i],
#         shares=json_data['Shares'][i],
#         # ... etc
#     )
#     trades.append(trade)

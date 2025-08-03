from sqlalchemy import Column, Integer, String, Float
from database import Base


class SalesData(Base):
    __tablename__ = 'sales_data'

    Order_ID = Column(Integer, primary_key=True, index=True)
    Product = Column(String(255))
    Categories = Column(String(255))
    Purchase_Address = Column(String(255))
    Quantity_Ordered = Column(Integer)
    Price_Each = Column(Float)
    Turnover = Column(Float)

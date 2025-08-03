from pydantic import BaseModel

class SalesOrderSchemas(BaseModel):

    Order_ID = int
    Product = str
    Categories = str
    Purchase_Address = str
    Quantity_Ordered = int
    Price_Each = float
    Turnover = float

    class Config:
        orm_mode = True


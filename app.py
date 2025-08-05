from http.client import HTTPException
from typing import List, Optional
from venv import logger
from fastapi import FastAPI, Query, Depends
from database import SessionLocal
from sqlalchemy.orm import Session
import actions
from database import Base, engine
from fastapi.responses import StreamingResponse

Base.metadata.create_all(bind=engine)

app = FastAPI()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

actions_obj = actions.actions()

@app.post("/chunking")
async def chunking(file_path: str):
    try:
        return await actions_obj.chunking(file_path)
    except Exception as e:
        return {"error": str(e)}
    
@app.post("/merging")
async def merging(root_folder: str):
    try:
        return await actions_obj.merging(root_folder)
    except FileNotFoundError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": "An unexpected error occurred: " + str(e)}
    
@app.post("/filter")
async def filter_sales(
        min_price: float = Query(0, description="Minimum price"),
        max_price: float = Query(1700, description="Maximum price"),
        Categories: Optional[List[str]] = Query(None, description="Category"),
        Product: Optional[List[str]] = Query(None, description="Product"),
        Purchase_Address: Optional[List[str]] = Query(None, description="Purchase Address"),
        Quantity_Ordered: Optional[List[int]] = Query(None, description="Quantity"),
        Price_Each: Optional[List[float]] = Query(None, description="Price Each"),
        Turnover: Optional[List[float]] = Query(None, description="Turnover"),
        db: Session = Depends(get_db)
):
    try:
        result = await actions_obj.filtering(
            db=db,
            min_price=min_price,
            max_price=max_price,
            Categories=Categories,
            Product=Product,
            Purchase_Address=Purchase_Address,
            Quantity_Ordered=Quantity_Ordered,
            Price_Each=Price_Each,
            Turnover=Turnover
        )
        return result
    except Exception as e:
        return {"error": "An error occurred while filtering: " + str(e)}
    finally:
        db.close()


@app.get("/get_sales_csv")
async def get_sales_csv(
        min_price: float = Query(0, description="Minimum price"),
        max_price: float = Query(1700, description="Maximum price"),
        Categories: Optional[List[str]] = Query(None, description="Category"),
        Product: Optional[List[str]] = Query(None, description="Product"),
        Purchase_Address: Optional[List[str]] = Query(None, description="Purchase Address"),
        Quantity_Ordered: Optional[List[int]] = Query(None, description="Quantity"),
        Price_Each: Optional[List[float]] = Query(None, description="Price Each"),
        Turnover: Optional[List[float]] = Query(None, description="Turnover"),
        db: Session = Depends(get_db)
):
    try:
        logger.info("Exporting data as csv")
        filter_data = await actions_obj.filtering(
            db=db,
            min_price=min_price,
            max_price=max_price,
            Categories=Categories,
            Product=Product,
            Purchase_Address=Purchase_Address,
            Quantity_Ordered=Quantity_Ordered,
            Price_Each=Price_Each,
            Turnover=Turnover
        )
        csv_stream = await actions_obj.convert_to_csv(filter_data)
        logger.info("Successfully generated CSV for download")

        return StreamingResponse(
            iter([csv_stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sales_orders.csv"}
        )
    except Exception as e:
        logger.error("Error exporting sales orders to CSV: %s", str(e), exc_info=True)
        return {"error": "An error occurred while exporting sales orders: " + str(e)}
    finally:
        db.close()
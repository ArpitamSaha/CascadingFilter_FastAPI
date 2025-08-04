from http.client import HTTPException
from typing import Optional
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
    
# @app.post("/filter")
# async def filter_sales(
#         min_price: float,
#         max_price: float,
#         category: str = None,
#         Product: str = None,
#         Address: str = None,
#         Quantity: int = None,
#         Turnover: float = None,
# ):
#     db = SessionLocal()
#     try:
#         result = await actions_obj.filter(
#             db=db,
#             min_price=min_price,
#             max_price=max_price,
#             category=category,
#             Product=Product,
#             Address=Address,
#             Quantity=Quantity,
#             Turnover=Turnover
#         )
#         return result
#     except Exception as e:
#         return {"error": "An error occurred while filtering: " + str(e)}
#     finally:
#         db.close()


@app.post("/filter")
def filter_sales(
        min_price: float = Query(0, description="Minimum price"),
        max_price: float = Query(1700, description="Maximum price"),
        Categories: Optional[str] = Query(None, description="Category"),
        Product: Optional[str] = Query(None, description="Product"),
        Purchase_Address: Optional[str] = Query(None, description="Purchase_Address"),
        Quantity: Optional[int] = Query(None, description="Quantity"),
        Turnover: Optional[float] = Query(None, description="Turnover"),
        db: Session = Depends(get_db)
):
    try:
        logger.info("Exporting data as csv")
        filter_data = actions_obj.filter(
            db=db,
            min_price=min_price,
            max_price=max_price,
            Categories=Categories,
            Product=Product,
            Purchase_Address=Purchase_Address,
            Quantity=Quantity,
            Turnover=Turnover
        )
        csv_stream = actions_obj.convert_to_csv(filter_data)
        logger.info("Successfully generated CSV for download")

        return StreamingResponse(
            iter([csv_stream.getvalue()]),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=sales_orders.csv"}
        )
    except Exception as e:
        logger.error("Error exporting sales orders to CSV: %s", str(e), exc_info=True)
        # raise HTTPException(status_code=500, detail="Error exporting sales orders.")
        return {"error": "An error occurred while exporting sales orders: " + str(e)}
    finally:
        db.close()
from fastapi import FastAPI
from database import SessionLocal
import actions
from database import Base, engine

Base.metadata.create_all(bind=engine)

app = FastAPI()

actions_obj = actions.actions()

@app.post("/chunking")
async def chunking(file_path: str):
    try:
        return await actions_obj.chunking(file_path)
    except Exception as e:
        return {"error": str(e)}
    
@app.post("/merging")
def merging(root_folder: str):
    try:
        return actions_obj.merging(root_folder)
    except FileNotFoundError as e:
        return {"error": str(e)}
    except Exception as e:
        return {"error": "An unexpected error occurred: " + str(e)}
    
@app.post("/filter")
def filter_sales(
        min_price: float,
        max_price: float,
        category: str = None,
        product: str = None,
        address: str = None,
        quantity: int = None,
        turnover: float = None,
):
    db = SessionLocal()
    try:
        result = actions_obj.filter(
            db=db,
            min_price=min_price,
            max_price=max_price,
            category=category,
            product=product,
            address=address,
            quantity=quantity,
            turnover=turnover
        )
        return result
    except Exception as e:
        return {"error": "An error occurred while filtering: " + str(e)}
    finally:
        db.close()
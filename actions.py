from io import StringIO
from operator import and_
from typing import List, Optional
from venv import logger
import pandas as pd
import os
from database import SessionLocal
from sqlalchemy.orm import Session
from sqlalchemy import and_
from models import SalesData
class actions:
    def __init__(self):
        self
    
    async def chunking(self,file_path):
        no_of_chunks, chunk_size= 3, 150
        df = pd.read_csv(file_path)
        breakpoint()
        for i in range(0, no_of_chunks):
            start = i * chunk_size
            end = start + chunk_size
            chunk = df.iloc[start:end]
            chunk.to_csv(f'Files/sales_data_chunk_{i+1}.csv', index=False)
            print(f'sales_data_chunk_{i+1}.csv saved with {len(chunk)} records.')
            
        return {"message": f"File chunked into {no_of_chunks} parts."}
        
    async def merging(self, root_folder):
        try:
            csv_list = []
            if not os.path.exists(root_folder):
                raise FileNotFoundError(f"The directory {root_folder} does not exist.")

            for foldername, subfolders, filenames in os.walk(root_folder):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        csv_list.append(os.path.join(foldername, filename))

            if not csv_list:
                raise FileNotFoundError("No CSV files found in the specified directory.")

            content = []
            for file_path in csv_list:
                df = pd.read_csv(file_path)
                df = df.drop_duplicates(subset=["Order ID"])
                content.append(df)
                print(f"Successfully read {file_path}")

            merged_df = pd.concat(content, ignore_index=True)
            merged_df.drop_duplicates(subset=["Order ID"], inplace=True)
            merged_df.to_csv('Files/merged_sales_data.csv', index=False)
            print("All CSV files merged into 'Files/merged_sales_data.csv'.")

            db = SessionLocal()
            try:
                for _, row in merged_df.iterrows():
                    order_id = int(row['Order ID'])

                    if db.query(SalesData).filter(SalesData.Order_ID == order_id).first():
                        print(f"Order ID {order_id} already exists. Skipping.")
                        continue

                    sales_data = SalesData(
                        Order_ID=order_id,
                        Product=str(row['Product']),
                        Categories=str(row.get('catégorie') or row.get('Categories') or "Unknown"),
                        Purchase_Address=str(row['Purchase_Address']),
                        Quantity_Ordered=int(row['Quantity Ordered']),
                        Price_Each=float(row['Price Each']),
                        Turnover=float(row['Turnover'])
                    )
                    db.add(sales_data)

                db.commit()
                print("Merged data inserted into the database.")
                return {"message": "Merged data inserted into the database."}

            except Exception as db_exc:
                print(f"Database error: {db_exc}")
                return {"error": f"Database error: {db_exc}"}
            finally:
                db.close()

        except FileNotFoundError as e:
            print(f"Error: {e}")
            return {"error": str(e)}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return {"error": "An unexpected error occurred: " + str(e)}
        
        # Alternative way suggest by Satvik
        # data_frame.to_sql('sales_data', con=connection, if_exists='replace', index=False)

    # async def filter(
    #     self,
    #     db: Session,
    #     min_price: float,
    #     max_price: float,
    #     Categories: Optional[List[str]] = None,
    #     Product: Optional[List[str]] = None,
    #     Purchase_Address: Optional[List[str]] = None,
    #     Quantity_Ordered: Optional[List[int]] = None,
    #     Price_Each: Optional[List[float]] = None,
    #     Turnover: Optional[List[float]] = None,
    # ):
    #     try:
    #         logger.info("Filtering sales data")
    #         filters = [
    #             SalesData.Price_Each >= min_price,
    #             SalesData.Price_Each <= max_price
    #         ]
    #         filter_map = {
    #             "Categories": (Categories, SalesData.Categories),
    #             "Product": (Product, SalesData.Product),
    #             "Purchase_Address": (Purchase_Address, SalesData.Purchase_Address),
    #             "Quantity_Ordered": (Quantity_Ordered, SalesData.Quantity_Ordered),
    #             "Price_Each": (Price_Each, SalesData.Price_Each),
    #             "Turnover": (Turnover, SalesData.Turnover),
    #         }
    #         for values, column in filter_map.values():
    #             if values:
    #                 filters.append(column.in_(values))
    
    #         result = db.query(SalesData).filter(and_(*filters)).all()
    #         logger.info("Filtered query returned %d records.", len(result))
    #         return result
    #     except Exception as e:
    #         logger.error("Error filtering sales data: %s", str(e), exc_info=True)
    #         return {"error": "An error occurred while filtering: " + str(e)}

    async def filtering(
        self,
        db: Session,
        min_price: float,
        max_price: float,
        Categories: Optional[List[str]] = None,
        Product: Optional[List[str]] = None,
        Purchase_Address: Optional[List[str]] = None,
        Quantity_Ordered: Optional[List[int]] = None,
        Price_Each: Optional[List[float]] = None,
        Turnover: Optional[List[float]] = None,
    ):
        try:
            logger.info("Filtering sales data")

            filters = [
                SalesData.Price_Each >= min_price,
                SalesData.Price_Each <= max_price
            ]

            filter_map = {
                "Categories": (Categories, SalesData.Categories),
                "Product": (Product, SalesData.Product),
                "Purchase_Address": (Purchase_Address, SalesData.Purchase_Address),
                "Quantity_Ordered": (Quantity_Ordered, SalesData.Quantity_Ordered),
                "Price_Each": (Price_Each, SalesData.Price_Each),
                "Turnover": (Turnover, SalesData.Turnover),
            }

            for values, column in filter_map.values():
                if values:
                    filters.append(column.in_(values))
            result = db.query(SalesData).filter(and_(*filters)).all()

            logger.info("Filtered query returned %d records.", len(result))
            return result

        except Exception as e:
            logger.error("Error filtering sales data: %s", str(e), exc_info=True)
            return {"error": f"An error occurred while filtering: {str(e)}"}

    async def convert_to_csv(
            self, filter_data
    ) -> StringIO:
        try:
            logger.info("Converting %d sales orders to CSV stream...", len(filter_data))
            df = pd.DataFrame([order.__dict__ for order in filter_data])
            df.drop(columns=["_sa_instance_state"], errors="ignore", inplace=True)
            csv_stream = StringIO()
            df.to_csv(csv_stream, index=False)
            csv_stream.seek(0)
            logger.info("CSV conversion complete.")
            return csv_stream
            
        except Exception as e:
            logger.error(f"Error converting to CSV: {e}")
            error_stream = StringIO()
            pd.DataFrame([{"error": f"Error converting to CSV: {str(e)}"}]).to_csv(error_stream, index=False)
            error_stream.seek(0)
            return error_stream
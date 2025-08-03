import pandas as pd
import os
from database import SessionLocal
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
        
    def merging(self, root_folder):
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
                        # skipped_count += 1
                        continue

                    sales_data = SalesData(
                        Order_ID=order_id,
                        Product=str(row['Product']),
                        Categories=str(row.get('catégorie') or row.get('Categories') or "Unknown"),
                        Purchase_Address=str(row['Purchase Address']),
                        Quantity_Ordered=int(row['Quantity Ordered']),
                        Price_Each=float(row['Price Each']),
                        Turnover=float(row['turnover'])
                    )
                    db.add(sales_data)
                    # inserted_count += 1

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

    def filter(
        self,
        db: SessionLocal,
        min_price: float,
        max_price: float,
        category: str = None,
        product: str = None,
        address: str = None,
        quantity: int = None,
        turnover: float = None
    ):
        query = db.query(SalesData).filter(SalesData.Price_Each >= min_price, SalesData.Price_Each <= max_price)
        if category:
            query = query.filter(SalesData.Categories == category)
        if product:
            query = query.filter(SalesData.Product == product)
        if address:
            query = query.filter(SalesData.Purchase_Address == address)
        if quantity:
            query = query.filter(SalesData.Quantity_Ordered == quantity)
        if turnover:
            query = query.filter(SalesData.Turnover == turnover)
        return query.all()
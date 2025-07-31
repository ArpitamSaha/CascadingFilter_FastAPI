import pandas as pd
import os
class actions:
    def __init__(self):
        self
    
    # async def chunking(self,file_path):
    #     no_of_chunks, chunk_size= 3, 150
    #     df = pd.read_csv(file_path)
    #     for i in range(0, no_of_chunks):
    #         start = i * chunk_size
    #         end = start + chunk_size
    #         chunk = df.iloc[start:end]
    #         chunk.to_csv(f'Files/sales_data_chunk_{i+1}.csv', index=False)
    #         print(f'sales_data_chunk_{i+1}.csv saved with {len(chunk)} records.')
            
    
    def merging(self,root_folder):
        try:
            csv_list = []
            if not os.path.exists(root_folder):
                raise FileNotFoundError(f"The directory {root_folder} does not exist.")
            for foldername, subfolders, filenames in os.walk(root_folder):
                for filename in filenames:
                    if filename.endswith('.csv'):
                        csv_list.append(f"{foldername}/{filename} ")
            if not csv_list:
                raise FileNotFoundError("No CSV files found in the specified directory.")
            
            content = []

            for file_path in csv_list:
                df = pd.read_csv(file_path)
                content.append(df)
                print(f"Successfully read {file_path}")
            
            merged_df = pd.concat(content, ignore_index=True)
            merged_df.to_csv('Files/merged_sales_data.csv', index=False)
            print("All CSV files merged into 'Files/merged_sales_data.csv'.")
            return "Merging completed successfully."
        
        except FileNotFoundError as e:
            print(f"Error: {e}")
            return {"error": str(e)}
        except Exception as e:
            print(f"An unexpected error occurred: {e}")
            return {"error": "An unexpected error occurred: " + str(e)}
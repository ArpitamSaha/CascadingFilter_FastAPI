import pandas as pd

df = pd.read_csv('sales_data.csv')

no_of_chunks = 3
chunk_size = 150

for i in range(0, no_of_chunks):
    start = i * chunk_size
    end = start + chunk_size
    chunk = df.iloc[start:end]
    chunk.to_csv(f'Files/sales_data_chunk_{i+1}.csv', index=False)
    print(f'sales_data_chunk_{i+1}.csv saved with {len(chunk)} records.')

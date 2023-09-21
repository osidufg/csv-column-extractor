# SEBELUM RUNNING CODE INI
# 1. open cmd > pip install pandas > pip install dask[dataframe]
# 2. konfigurasi filename, delimiter, dan kolom yang dipilih
# 3. file csv harus satu directory dengan file python
# 4. kalo ada masalah memory, langsung paralel pake dask
import os, pandas as pd, dask.dataframe as dd

# konfigurasi
threshold_mb = 1000 # >1 GB pindah ke dask
input_filename = 'input_csv.csv'
output_filename = 'extracted_csv.csv'
delimiter = ','
selected_column = [
    'column1',
    'column2',
    'column3'
]

# code
def size_treshold(size,treshold):
    return size > treshold
script_path = os.path.dirname(os.path.abspath(__file__))
csv_path = os.path.join(script_path, input_filename) 
file_size_mb = os.path.getsize(csv_path) / (1024 ** 2)

try:
    if size_treshold(file_size_mb,threshold_mb):
        csv_df = dd.read_csv(csv_path, sep=delimiter, usecols=selected_column)
    else:
        csv_df = pd.read_csv(csv_path, sep=delimiter, usecols=selected_column)
    if size_treshold(file_size_mb,threshold_mb): 
        csv_df.to_csv(output_filename,columns=selected_column, sep=delimiter, single_file=True, index=False)
    else:
       csv_df.to_csv(output_filename,columns=selected_column, sep=delimiter, index=False)
except Exception as e:
    print(f'- - - x ERROR x - - -\n{e}\n\n')
else:
    print(f'- - - + DONE + - - -\n[DataFrame Library Used]\n{"Dask" if size_treshold(file_size_mb,threshold_mb) else "Pandas"}\n\n[Selected Column]\n{", ".join(selected_column)}\n\n[Output directory]\n{script_path}\\{output_filename}\n\n')
# commented karena ga perlu (kalo running di vs code)
# finally:
#     input("Press enter to exit...")
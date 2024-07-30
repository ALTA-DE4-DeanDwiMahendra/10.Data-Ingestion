import pandas as pd
def ingest_data(file_path):
    try:
        # Membaca file CSV
        data = pd.read_csv(file_path)
        # Menampilkan beberapa baris pertama data
        print(data.head())
    except Exception as e:
        print(f"Terjadi kesalahan saat ingestion data: {e}")
if __name__ == "__main__":
    file_path = "sample.csv"
    ingest_data(file_path)

import pandas as pd
from sqlalchemy import create_engine, text

def ingest_data(file_path, db_url):
    try:
        # Membaca file CSV
        data = pd.read_csv(file_path)
        
        # Membuat koneksi ke PostgreSQL
        engine = create_engine(db_url)
        
        # Mengunggah data ke tabel PostgreSQL (misalnya, tabel 'deals')
        data.to_sql('deals', engine, if_exists='replace', index=False)
        
        # Menghitung jumlah baris yang diingest
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM deals"))
            row_count = result.fetchone()[0]
        
        print(f"Ingestion data berhasil. Jumlah baris yang diingest: {row_count}")
    except Exception as e:
        print(f"Terjadi kesalahan saat ingestion data: {e}")

if __name__ == "__main__":
    # Menentukan path ke file CSV
    file_path = "sample.csv"
    # Menentukan URL database PostgreSQL
    db_url = "postgresql://postgres:080220@localhost:5432/postgres"
    ingest_data(file_path, db_url)

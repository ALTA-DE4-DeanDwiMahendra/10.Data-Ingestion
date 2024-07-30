import pandas as pd
from sqlalchemy import create_engine, text

def ingest_data(file_path, db_url):
    try:
        # Membaca file JSON (gunakan lines=True jika file JSON Lines)
        data = pd.read_json(file_path, lines=True)
        print("Berhasil membaca file JSON.")
        
        # Membuat koneksi ke PostgreSQL
        engine = create_engine(db_url)
        print("Berhasil terhubung ke PostgreSQL.")
        
        # Mengunggah data ke tabel PostgreSQL
        data.to_sql('task_day3', engine, if_exists='replace', index=False)
        print("Berhasil mengunggah data ke PostgreSQL.")
        
        # Menghitung jumlah baris yang diingest
        with engine.connect() as connection:
            result = connection.execute(text("SELECT COUNT(*) FROM task_day3"))
            row_count = result.fetchone()[0]
        
        print(f"Ingestion data berhasil. Jumlah baris yang diingest: {row_count}")
        
        # Menampilkan data yang diingest
        with engine.connect() as connection:
            result = connection.execute(text("SELECT * FROM task_day3 LIMIT 5"))
            for row in result:
                print(row)
        
    except Exception as e:
        print(f"Terjadi kesalahan saat ingestion data: {e}")

if __name__ == "__main__":
    # Tentukan path ke file JSON Anda
    file_path = "2017-10-02-1.json"
    # Tentukan URL database PostgreSQL Anda
    db_url = "postgresql://postgres:080220@localhost:5432/postgres"
    ingest_data(file_path, db_url)
from pyspark.sql import SparkSession
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
from pyspark.sql.functions import col
from pyspark.sql.functions import mean

# Inisialisasi sesi Spark
spark = SparkSession.builder.appName("StrokeAnalysis").getOrCreate()

# Baca data dari file CSV
file_path = "dataset-penyakit-stroke-di-indonesia.csv"
df = spark.read.csv(file_path, header=True, inferSchema=True)

# Tampilkan skema data
df.printSchema()

df.columns

df.count()

#menghitung jumlah kolom dalam DataFrame 
len(df.columns)

selected_columns = ['hipertensi', 'tingkat glukosa', 'bmi', 'status_merokok']
df_selected = df[selected_columns]


# Tampilkan DataFrame yang sudah dipilih
df_selected.show()

df.filter(df['bmi']>30.0).show()

# Konversi DataFrame PySpark ke Pandas DataFrame
pandas_df = df.toPandas()

# histogram untuk distribusi usia
plt.figure(figsize=(10, 6))
sns.histplot(pandas_df['usia'], bins=30, kde=True, color='blue')
plt.title('Distribusi Usia Pasien')
plt.xlabel('Usia')
plt.ylabel('Frekuensi')
plt.show()

# Visualisasi data menggunakan Seaborn
plt.figure(figsize=(8, 5))
sns.countplot(x='jk', data=pandas_df, hue='jk', palette='pastel', legend=False)
plt.title('Jumlah Pasien Berdasarkan Jenis Kelamin')
plt.xlabel('Jenis Kelamin')
plt.ylabel('Jumlah')
plt.show()

# Mengelompokkan data berdasarkan jenis kelamin dan menghitung jumlah stroke
result = df.groupBy("jk").agg({"stroke": "sum"}).toPandas()

# Visualisasi Jumlah Stroke berdasarkan Jenis Kelamin
plt.figure(figsize=(8, 6))
sns.barplot(x="jk", y="sum(stroke)", data=result)
plt.title("Jumlah Stroke berdasarkan Jenis Kelamin")
plt.xlabel("Jenis Kelamin")
plt.ylabel("Jumlah Stroke")
plt.show()


# Visualisasi data menggunakan Seaborn
plt.figure(figsize=(8, 5))
sns.countplot(x='penyakit_jantung', data=pandas_df, hue='penyakit_jantung', palette='pastel', legend=False)
plt.title('Distribusi Pasien Berdasarkan Penyakit Jantung')
plt.xlabel('Penyakit Jantung')
plt.ylabel('Jumlah')
plt.show()

# Visualisasi data berdasarkan status merokok menggunakan Seaborn
plt.figure(figsize=(8, 5))
sns.countplot(x='status_merokok', data=pandas_df, hue='stroke', palette='pastel')
plt.title('Distribusi Pasien Berdasarkan Status Merokok')
plt.xlabel('Status Merokok')
plt.ylabel('Jumlah')
plt.show()



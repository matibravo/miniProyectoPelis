import polars as pl
import time

print("iniciando proyecto")

start = time.time()

# Leer el archivo CSV

try:
    archivo_csv = pl.read_csv("./peliculas.csv")
    print(archivo_csv)
except Exception as e:
    print(f"Error al leer el archivo csv: {e}")
    exit()

# Mostrar el promedio de duración (runtime) por género.
print("Mostrar el promedio de duración (runtime) por género.")

query_promedio = archivo_csv.group_by(
    pl.col("genre"),
).agg(
    pl.len().alias("cantidad peliculas por genero"),
    pl.col("runtime").mean().alias("promedio")
)

print(query_promedio)

# Calcular el ROI(retorno de inversion) (revenue / budget) de cada película y ordenarlas de mayor a menor.
print("Calcular el ROI (revenue / budget) de cada película y ordenarlas de mayor a menor.")

archivo_csv = archivo_csv.with_columns(
    roi = ((pl.col("revenue") - pl.col("budget")) / (pl.col("budget")) * 100)
).sort(by="roi", descending=True)

print(archivo_csv)

# Contar cuántas películas hay por década.
print("Contar cuántas películas hay por década.")

query_decada = archivo_csv.group_by(    
    (pl.col("year") // 10 * 10).alias("decada")
).agg(
    pl.len(),
    pl.col("title")
)

print(query_decada)

# Mostrar el promedio de rating por género.
print("Mostrar el promedio de rating por género.")

query_promedio_rating = archivo_csv.group_by(
    pl.col("genre")
).agg(
    pl.len().alias("cantidad"),
    pl.col("rating").mean().name.prefix("average_")
)

print(query_promedio_rating)

# Filtrar las películas con rating mayor a 8.5 y duración menor a 150 minutos.
print("Filtrar las películas con rating mayor a 8.5 y duración menor a 150 minutos.")
df_filtrado = archivo_csv.filter(
    (pl.col("rating") > 8.5) & (pl.col("runtime") < 150)
)

print(df_filtrado)

#  Crear una nueva columna que indique si una película fue "Exitosa" (ROI > 500).

query_add_column = archivo_csv.with_columns(
    exitosa = pl.col("roi") > 500
)

print(query_add_column)

query_add_column.write_csv("PeliculasModificado.csv")
print(f"tiempo transcurrido: {time.time() - start} segundos")
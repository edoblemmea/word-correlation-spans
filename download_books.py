import pandas as pd
import requests
import os
import time

# Configuración
IDIOMAS = {
    "en": "english",
    "fr": "french",
    "fi": "finnish",
    "zh": "chinese",
    "ja": "japanese",
    "tl": "tagalog"
}
LIBROS_POR_IDIOMA = 100
CARPETA_SALIDA = "data/raw"

os.makedirs(CARPETA_SALIDA, exist_ok=True)

# Cargar metadatos
print("Cargando metadatos...")
#df = pd.read_csv("SPGC-metadata-2018-07-18.csv")
df = pd.read_csv("SPGC-metadata-2018-07-18.csv", 
                 quotechar='"',
                 on_bad_lines='skip')
print(f"Total libros en metadatos: {len(df)}")
print(f"Columnas disponibles: {df.columns.tolist()}")

# Filtrar por idiomas seleccionados
'''
df_filtrado = (
    df[df["language"].isin(IDIOMAS.keys())]
    .groupby("language")
    .head(LIBROS_POR_IDIOMA)
    .reset_index(drop=True)
)

print("\nLibros seleccionados por idioma:")
print(df_filtrado["language"].value_counts())
print(f"\nTotal a descargar: {len(df_filtrado)}")
'''

# Filtrar por idiomas seleccionados
# La columna language tiene formato "['en']" en lugar de "en"
# Por eso usamos str.contains en lugar de isin

def idioma_coincide(lang_str, idiomas):
    if pd.isna(lang_str):
        return False
    return any(f"'{codigo}'" in str(lang_str) for codigo in idiomas)

df['lang_code'] = df['language'].apply(
    lambda x: next(
        (codigo for codigo in IDIOMAS.keys() if f"'{codigo}'" in str(x)), 
        None
    )
)

df_filtrado = (
    df[df['lang_code'].notna()]
    .groupby('lang_code')
    .head(LIBROS_POR_IDIOMA)
    .reset_index(drop=True)
)

print("\nLibros seleccionados por idioma:")
print(df_filtrado["lang_code"].value_counts())
print(f"\nTotal a descargar: {len(df_filtrado)}")

# Función para intentar diferentes URLs de descarga
def descargar_libro(pg_id):
    num = str(pg_id)
    
    # Posibles URLs donde puede estar el libro
    urls_candidatas = [
        f"https://www.gutenberg.org/files/{num}/{num}-0.txt",
        f"https://www.gutenberg.org/files/{num}/{num}.txt",
        f"https://www.gutenberg.org/cache/epub/{num}/pg{num}.txt",
    ]
    
    for url in urls_candidatas:
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200:
                return response.text, url
        except requests.exceptions.RequestException:
            continue
    
    return None, None

# Descargar libros
descargados = 0
fallidos = 0

for _, row in df_filtrado.iterrows():
    pg_id = row["id"].replace("PG", "")  # eliminar el prefijo PG
    idioma = IDIOMAS.get(row["lang_code"], row["lang_code"])
    titulo = row.get("title", "sin_titulo")
    
    archivo_salida = os.path.join(CARPETA_SALIDA, f"PG{pg_id}_raw.txt")
    
    # Saltar si ya está descargado
    if os.path.exists(archivo_salida):
        print(f"[EXISTE] PG{pg_id} ({idioma})")
        descargados += 1
        continue
    
    print(f"[DESCARGANDO] PG{pg_id} ({idioma}) - {titulo[:50]}")
    
    texto, url_usada = descargar_libro(pg_id)
    
    if texto:
        with open(archivo_salida, "w", encoding="utf-8") as f:
            f.write(texto)
        print(f"  ✓ Guardado desde {url_usada}")
        descargados += 1
    else:
        print(f"  ✗ No se pudo descargar PG{pg_id}")
        fallidos += 1
    
    # Pausa para no saturar el servidor
    time.sleep(1)

print(f"\n--- RESUMEN ---")
print(f"Descargados: {descargados}")
print(f"Fallidos:    {fallidos}")
print(f"Guardados en: {CARPETA_SALIDA}")
from fastapi import FastAPI, HTTPException
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa


app = FastAPI()

df_developer = pq.read_table("datasets/Parquet/EP_developer.parquet").to_pandas()
df_userdata = pq.read_table("datasets/Parquet/EP_userdata.parquet").to_pandas()
df_userforgenre = pq.read_table("datasets/Parquet/EP_userforgenre.parquet").to_pandas()
df_bestdeveloperyear = pq.read_table("datasets/Parquet/EP_bestdeveloperyear.parquet").to_pandas()
df_developeranalysis = pq.read_table("datasets/Parquet/EP_developeranalysis.parquet").to_pandas()
df_recomendacion = pq.read_table("datasets/Parquet/EP_ML_recomendacion.parquet").to_pandas()


# Define el endpoint de FastAPI
@app.get("/developer")
async def get_developer_info(developer_name: str):
    """
    Función para obtener información sobre los juegos desarrollados por un desarrollador específico.

    Args:
        developer_name (str): El nombre del desarrollador a buscar.

    Returns:
        dict: Un diccionario con información sobre los juegos desarrollados por el desarrollador especificado.
    """
    # Convertir el nombre del desarrollador a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    developer_name_lower = developer_name.lower()
    
    # Filtrar el DataFrame por el nombre del desarrollador
    df_filtered = df_developer[df_developer['developer'].str.lower() == developer_name_lower]
    
    # Si no se encuentra ninguna coincidencia, devolver un error HTTP 404
    if df_filtered.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron juegos desarrollados por '{developer_name}'.")
    
    # Agrupar por año y contar el número de elementos por grupo
    grouped = df_filtered.groupby('release_year').agg({'developer': 'count', 'price': lambda x: (x == 0).sum()})
    grouped.columns = ['Cantidad de Items', 'Contenido Free']
    
    # Calcular el porcentaje de contenido gratuito
    grouped['Contenido Free'] = (grouped['Contenido Free'] / grouped['Cantidad de Items'] * 100).round(2).astype(str) + '%'
    
    # Renombrar la primera columna como "Year"
    grouped.index.name = 'Year'
    
    # Convertir el resultado a un formato JSON compatible
    result = grouped.to_dict(orient='index')
    
    return result

# Define el endpoint de FastAPI
@app.get("/userdata")
async def get_user_data(user_id: str):
    """
    Función para obtener información sobre los juegos adquiridos por un usuario específico.

    Args:
        user_id (str): El identificador único del usuario del que se desean obtener los datos.

    Returns:
        dict: Un diccionario con información sobre los juegos adquiridos por el usuario especificado.
    """
    # Convertir el user_id a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    user_id_lower = user_id.lower()
    
    # Filtrar el DataFrame por el user_id dado
    user_items = df_userdata[df_userdata['user_id'].str.lower() == user_id_lower]

    # Verificar si se encontraron items para el user_id dado
    if user_items.empty:
        raise HTTPException(status_code=404, detail=f"No se encontraron items para el usuario {user_id}")
    
    # Calcular el porcentaje de recomendación
    recommend_percentage = user_items['recommend'].mean() * 100
    
    # Calcular el total gastado por el usuario
    total_spent = int(user_items['price'].sum())
    
    # Contar la cantidad de items del usuario
    num_items = len(user_items)
    
    # Crear el diccionario de resultados
    result = {
        "Usuario": user_id,
        "Dinero gastado": f"{total_spent} USD",
        "% de recomendación": f"{recommend_percentage:.2f}%",
        "Cantidad de items": num_items
    }
    
    return result

# Define el endpoint de FastAPI
@app.get("/user_for_genre")
async def get_user_for_genre(genre: str):
    """
    Busca al usuario que ha acumulado más horas jugadas en un género específico y devuelve una lista de la acumulación de horas jugadas por año de lanzamiento para ese género.

    Args:
        genre (str): El género para el cual se quiere encontrar al usuario con más horas jugadas.

    Returns:
        dict: Un diccionario con dos claves: 'Usuario con más horas jugadas para [Género]' y 'Horas jugadas'.
    """
    # Convertir el género a minúsculas para hacer la comparación insensible a mayúsculas y minúsculas
    genre_lower = genre.lower()
    
    # Filtrar el DataFrame por el género especificado
    genre_data = df_userforgenre[df_userforgenre['genres'].apply(lambda x: genre_lower in [g.lower() for g in x])]
    
    # Verificar si no se encontraron datos de horas jugadas para el género especificado
    if genre_data.empty:
        raise HTTPException(status_code=404, detail="No se encontraron datos. Verifica si el dato ingresado es correcto")
    
    # Obtener el usuario con más horas jugadas para el género dado
    top_user = genre_data.groupby('user_id')['playtime_forever'].sum().idxmax()
    
    # Filtrar las horas jugadas por el usuario con más horas jugadas para el género dado
    top_user_data = genre_data[genre_data['user_id'] == top_user]
    
    # Obtener la acumulación de horas jugadas por año de lanzamiento
    year_hours = top_user_data.groupby('release_year')['playtime_forever'].sum()
    
    # Ordenar la lista de diccionarios por el valor del año de lanzamiento de forma descendente
    year_hours_list = [{'Año': year, 'Horas': hours} for year, hours in sorted(year_hours.items(), key=lambda x: x[0], reverse=True)]

    return {"Usuario con más horas jugadas para " + genre: top_user, "Horas jugadas": year_hours_list}

# Define el endpoint de FastAPI
@app.get("/best_developer_year")
async def get_best_developer_year(year: int):
    """
    Devuelve el top 3 de desarrolladores con juegos más recomendados por usuarios para el año dado.

    Args:
        year (int): El año para el cual se quiere obtener el top 3 de desarrolladores.

    Returns:
        list: Una lista con los tres principales desarrolladores para el año dado, en formato de diccionarios donde cada uno tiene una clave 'Puesto' seguida del número de puesto y el valor del desarrollador.
    """
    # Filtrar los juegos por el año dado y por recomendaciones positivas (recommend=True) y sentimiento positivo (sentiment_analysis=2)
    filtered_data = df_bestdeveloperyear[(df_bestdeveloperyear['posted_year'] == year) & (df_bestdeveloperyear['recommend'] == True) & (df_bestdeveloperyear['sentiment_analysis'] == 2)]
    
    # Comprobar si no hay datos para el año especificado
    if filtered_data.empty:
        raise HTTPException(status_code=404, detail="No se encontraron datos para el año especificado.")
    
    # Contar las recomendaciones por desarrollador
    developer_counts = filtered_data['developer'].value_counts().reset_index(name='Count')
    
    # Obtener el top 3 de desarrolladores con juegos más recomendados para el año dado
    top_developers = developer_counts.nlargest(3, 'Count')

    # Extraer el nombre del desarrollador de cada fila y convertirlo en una lista
    developers_list = top_developers['developer'].tolist()

    # Formatear el resultado en el formato deseado
    result = [{"Puesto " + str(index + 1): developer} for index, developer in enumerate(developers_list)]

    return result

# Define el endpoint de FastAPI
@app.get("/developer_reviews_analysis")
async def get_developer_reviews_analysis(developer: str):
    """
    Analiza las revisiones de los usuarios para un desarrollador específico y devuelve un diccionario con la cantidad total de registros de reseñas de usuarios clasificados como positivos o negativos.

    Args:
        developer (str): El nombre del desarrollador del que se analizarán las revisiones.

    Returns:
        dict: Un diccionario con el nombre del desarrollador como clave y una lista con la cantidad total de registros de reseñas de usuarios clasificados como positivos o negativos.

    Si el desarrollador especificado no existe en los datos, la función devuelve un error HTTP 404.

    Ejemplo de retorno:
    {'Valve': {'Negative': 930, 'Positive': 5410}}
    """

    # Convertir el nombre del desarrollador proporcionado a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    developer_lower = developer.lower()

    # Copiar el DataFrame para evitar SettingWithCopyWarning
    df_developeranalysis_copy = df_developeranalysis.copy()

    # Convertir todos los nombres de desarrolladores en el DataFrame a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    df_developeranalysis_copy['developer'] = df_developeranalysis_copy['developer'].str.lower()

    if developer_lower not in df_developeranalysis_copy['developer'].unique():
        raise HTTPException(status_code=404, detail="No se encontraron datos para el desarrollador especificado.")

    # Filtrar las reviews por el desarrollador dado
    filtered_reviews = df_developeranalysis_copy[df_developeranalysis_copy['developer'] == developer_lower]

    # Filtrar las reviews por sentimiento positivo (2) y negativo (0)
    positive_reviews = filtered_reviews[filtered_reviews['sentiment_analysis'] == 2]
    negative_reviews = filtered_reviews[filtered_reviews['sentiment_analysis'] == 0]

    # Contar la cantidad de reviews positivas y negativas
    positive_count = len(positive_reviews)
    negative_count = len(negative_reviews)

    # Crear el diccionario con el resultado
    resultado = {developer: {'Negative': negative_count, 'Positive': positive_count}}

    return resultado

@app.get("/recomendacion_juego/{id_juego}")
async def get_recomendacion_juego(id_juego: int):
    """
    Función que devuelve las recomendaciones de juegos para un juego específico según su ID.

    Parámetros:
        id_juego (int): El ID del juego del cual se desean obtener las recomendaciones.

    Retorna:
        list: Una lista de nombres de juegos recomendados para el juego especificado por su ID.
              Si no se encuentran recomendaciones para el ID dado, se devuelve un mensaje indicando que no se encontraron recomendaciones.
    """
    try:
        # Buscar las recomendaciones para el juego especificado por su ID
        recomendaciones = df_recomendacion[df_recomendacion['id'] == id_juego]['recomendaciones'].values[0]
        return list(recomendaciones)
    except IndexError:
        # Si no se encuentra el juego en el DataFrame de recomendaciones, devolver un mensaje indicando que no se encontraron recomendaciones
        raise HTTPException(status_code=404, detail=f"No se encontraron recomendaciones para el ID {id_juego}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

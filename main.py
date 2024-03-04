from fastapi import FastAPI
import pandas as pd

app = FastAPI()

df_developer = pd.read_parquet("datasets/Parquet/endpoint_developer.parquet")
prices = pd.read_parquet("datasets/Parquet/endpoint_userdata_prices.parquet")
reviews = pd.read_parquet("datasets/Parquet/endpoint_userdata_reviews.parquet")
df_genre_years = pd.read_parquet("datasets/Parquet/endpoint_userforgenre.parquet")
df_merged = pd.read_parquet("datasets/Parquet/endpoint_bestdeveloperyear.parquet")
df_review_analysis = pd.read_parquet("datasets/Parquet/endpoint_developerreviewanalysis.parquet")

def developer(developer_name, df=df_developer):
    # Convertir el nombre del desarrollador a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    developer_name = developer_name.lower()
    
    # Filtrar el DataFrame por el nombre del desarrollador
    df_filtered = df[df['developer'].str.lower() == developer_name]
    
    # Si no se encuentra ninguna coincidencia, devolver un mensaje personalizado
    if df_filtered.empty:
        return f"No se encontraron juegos desarrollados por '{developer_name}'."
    
    # Agrupar por año y contar el número de elementos por grupo
    grouped = df_filtered.groupby('release_year').agg({'app_name': 'count', 'price': lambda x: (x == 0).sum()})
    grouped.columns = ['Cantidad de Items', 'Contenido Free']
    
    # Calcular el porcentaje de contenido gratuito
    grouped['Contenido Free'] = (grouped['Contenido Free'] / grouped['Cantidad de Items'] * 100).round(2).astype(str) + '%'
    
    # Renombrar la primera columna como "Year"
    grouped.index.name = 'Year'
    
    return grouped

@app.get("/developer_info/{developer_name}")
async def get_developer_info(developer_name: str):
    return developer(developer_name)

def userdata(user_id, user_game_prices=prices, df_reviews=reviews):
    """
    Esta función calcula la cantidad de dinero gastado por un usuario en juegos, el porcentaje de recomendación
    y la cantidad de items que ha adquirido.

    Parámetros:
    user_id (str): El identificador único del usuario del que se desean obtener los datos.
    user_game_prices (DataFrame): DataFrame que contiene los precios de los juegos para el usuario.
    df_reviews (DataFrame): DataFrame que contiene las reseñas de los juegos, incluyendo el indicador de recomendación.

    Retorna:
    dict: Un diccionario que contiene la siguiente información:
          - 'Usuario': El identificador único del usuario.
          - 'Dinero gastado': La cantidad total de dinero gastado por el usuario en juegos.
          - '% de recomendación': El porcentaje de recomendación promedio dado por el usuario en sus reseñas.
          - 'Cantidad de items': El número total de juegos adquiridos por el usuario.
    """

    user_id = user_id.lower()

    # Filtrar df_items por el user_id dado
    user_items = user_game_prices[user_game_prices['user_id'] == user_id]

    # Verificar si se encontraron items para el user_id dado
    if user_items.empty:
        return f"No se encontraron items para el usuario {user_id}"
    
    # Obtener los item_ids para el usuario
    item_ids = user_items['item_id'].tolist()
    
    # Filtrar df_reviews por los item_ids del usuario
    user_reviews = df_reviews[df_reviews['item_id'].isin(item_ids)]
    
    # Calcular el porcentaje de recomendación
    recommend_percentage = user_reviews['recommend'].mean() * 100
    
    # Calcular el total gastado por el usuario
    total_spent = int(user_items['price'].sum())
    
    # Contar la cantidad de items del usuario
    num_items = len(item_ids)
    
    # Crear el diccionario de resultados
    result = {
        "Usuario": user_id,
        "Dinero gastado": f"{total_spent} USD",
        "% de recomendación": f"{recommend_percentage:.2f}%",
        "Cantidad de items": num_items
    }
    
    return result

@app.get("/user_info/{user_id}")
async def get_user_info(user_id: str):
    return userdata(user_id)

def UserForGenre(genre, user_genre_years=df_genre_years):
    """
    Esta función busca al usuario que ha acumulado más horas jugadas en un género específico y devuelve una lista de la acumulación de horas jugadas por año de lanzamiento para ese género.

    Parámetros:
    - genre (str): El género para el cual se quiere encontrar al usuario con más horas jugadas.
    - user_genre_years (DataFrame): El DataFrame que contiene información sobre las horas jugadas, los géneros y los años de lanzamiento de los juegos.

    Retorna:
    - dict: Un diccionario con dos claves:
        - 'Usuario con más horas jugadas para [Género]': El usuario que ha acumulado más horas jugadas en el género especificado.
        - 'Horas jugadas': Una lista de diccionarios donde cada uno contiene el año de lanzamiento y las horas jugadas acumuladas para ese año en el género dado.

    Ejemplo de uso:
    >>> prueba = UserForGenre("Action")
    >>> print(prueba)
    {'Usuario con más horas jugadas para Action': 'us213ndjss09sdf', 'Horas jugadas': [{'Año': 2013, 'Horas': 203}, {'Año': 2012, 'Horas': 100}, {'Año': 2011, 'Horas': 23}]}
    """
    
    # Convertir el género a minúsculas para hacer la comparación insensible a mayúsculas y minúsculas
    genre_lower = genre.lower()
    
    # Filtrar los juegos por el género especificado
    genre_games = user_genre_years[user_genre_years['genres'].apply(lambda x: genre_lower in [g.lower() for g in x])]
    
    # Verificar si no se encontraron datos de horas jugadas para el género especificado
    if genre_games.empty:
        return {"No se encontraron datos. Verifica si el dato ingresado es correcto"}
    
    # Agrupar por usuario y sumar las horas jugadas
    user_hours = genre_games.groupby('user_id')['playtime_forever'].sum().reset_index()
    
    # Obtener el usuario con más horas jugadas para el género dado
    top_user = user_hours.loc[user_hours['playtime_forever'].idxmax()]['user_id']
    
    # Filtrar las horas jugadas por el usuario con más horas jugadas para el género dado
    top_user_hours = genre_games[genre_games['user_id'] == top_user]
    
    # Obtener la acumulación de horas jugadas por año de lanzamiento
    year_hours = top_user_hours.groupby('release_year')['playtime_forever'].sum()
    year_hours_list = [{'Año': year, 'Horas': hours} for year, hours in year_hours.items()]

    return {"Usuario con más horas jugadas para " + genre: top_user, "Horas jugadas": year_hours_list}

@app.get("/user_genre_info/{genre}")
async def get_user_genre_info(genre: str):
    return UserForGenre(genre)

def best_developer_year(year, merged_data=df_merged):
    """
    Esta función devuelve el top 3 de desarrolladores con juegos más recomendados por usuarios para el año dado.

    Parámetros:
    - year (int): El año para el cual se quiere obtener el top 3 de desarrolladores.
    - merged_data (DataFrame, opcional): El DataFrame que contiene las revisiones de los juegos unido con los datos de los juegos y con las columnas seleccionadas.

    Retorna:
    - list: Una lista con los tres principales desarrolladores para el año dado, en formato de diccionarios donde cada uno tiene una clave 'Puesto' seguida del número de puesto y el valor del desarrollador.

    Ejemplo de uso:
    >>> best_developer_year(2022)
    [{'Puesto 1': 'DeveloperX'}, {'Puesto 2': 'DeveloperY'}, {'Puesto 3': 'DeveloperZ'}]
    """
    # Filtrar las reviews por el año dado y por recomendaciones positivas (recommend=True) y sentimiento positivo (sentiment_analysis=2)
    filtered_reviews = merged_data[(merged_data['posted_year'] == year) & 
                                   (merged_data['recommend'] == True) & 
                                   (merged_data['sentiment_analysis'] == 2)]
    
    # Comprobar si no hay datos para el año especificado
    if filtered_reviews.empty:
        return "No se encontraron datos para el año especificado."
    
    # Contar las recomendaciones por desarrollador
    developer_counts = filtered_reviews.groupby('developer').size().reset_index(name='Count')
    
    # Obtener el top 3 de desarrolladores con juegos más recomendados para el año dado
    top_developers = developer_counts.nlargest(3, 'Count')

    # Extraer el nombre del desarrollador de cada fila y convertirlo en una lista
    developers_list = top_developers['developer'].tolist()

    # Formatear el resultado en el formato deseado
    result = [{"Puesto " + str(index + 1): developer} for index, developer in enumerate(developers_list)]

    # Devolver el resultado
    return result

@app.get("/best_developer/{year}")
async def get_best_developer(year: int):
    return best_developer_year(year)

def developer_reviews_analysis(developer, merged_data=df_review_analysis):
    """
    Analiza las revisiones de los usuarios para un desarrollador específico y devuelve un diccionario con el nombre del desarrollador como clave y una lista con la cantidad total de registros de reseñas de usuarios clasificados como positivos o negativos.

    Parámetros:
    - developer (str): El nombre del desarrollador del que se analizarán las revisiones.
    - merged_data (DataFrame): El DataFrame que contiene las revisiones de los usuarios unido con los datos de los juegos y con las columnas seleccionadas.

    Retorna:
    - dict: Un diccionario con el nombre del desarrollador como clave y una lista con la cantidad total de registros de reseñas de usuarios clasificados como positivos o negativos.

    Si el desarrollador especificado no existe en los datos, la función devuelve un mensaje indicando que no se encontraron datos para ese desarrollador.

    Ejemplo de retorno:
    {'Valve': {'Negative': 930, 'Positive': 5410}}
    """

    # Convertir el nombre del desarrollador proporcionado a minúsculas para una comparación sin distinción entre mayúsculas y minúsculas
    developer_lower = developer.lower()

    # Comprobar si el desarrollador existe en los datos
    if developer_lower not in merged_data['developer'].unique():
        return "No se encontraron datos para el desarrollador especificado."

    # Filtrar las reviews por el desarrollador dado
    filtered_reviews = merged_data[merged_data['developer'] == developer_lower]

    # Filtrar las reviews por sentimiento positivo (2) y negativo (0)
    positive_count = len(filtered_reviews[filtered_reviews['sentiment_analysis'] == 2])
    negative_count = len(filtered_reviews[filtered_reviews['sentiment_analysis'] == 0])

    # Crear el diccionario con el resultado
    resultado = {developer: {'Negative': negative_count, 'Positive': positive_count}}

    return resultado

@app.get("/developer_reviews/{developer}")
async def get_developer_reviews(developer: str):
    return developer_reviews_analysis(developer)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
![](https://cdn.cloudflare.steamstatic.com/store/home/store_home_share.jpg)

![Pandas](https://img.shields.io/badge/-Pandas-333333?style=flat&logo=pandas)![Numpy](https://img.shields.io/badge/-Numpy-333333?style=flat&logo=numpy)![Matplotlib](https://img.shields.io/badge/-Matplotlib-333333?style=flat&logo=matplotlib)![Seaborn](https://img.shields.io/badge/-Seaborn-333333?style=flat&logo=seaborn)![Scikitlearn](https://img.shields.io/badge/-Scikitlearn-333333?style=flat&logo=scikitlearn)![FastAPI](https://img.shields.io/badge/-FastAPI-333333?style=flat&logo=fastapi)![Render](https://img.shields.io/badge/-Render-333333?style=flat&logo=render)
 # PI_1-MLOps Juegos Steam - Modelo de recomendación     
**Proyecto Individual Nº1**

En este readme describiré las diferentes etapas y componentes del proyecto, detallando cómo se han abordado los requisitos y las soluciones implementadas.

### Descripción del proyecto:
El proyecto MLOps es una iniciativa integral que combina las habilidades de un Data Scientist y un Data Engineer para desarrollar un producto mínimo viable (MVP) en el ámbito de los videojuegos. Nestro objetivo es desplegar una API funcional que permita acceder a los datos de manera eficiente. Además, implementaremos un modelo de machine learning para realizar análisis de sentimientos basado en los comentarios de los usuarios. Este análisis permitirá categorizar las opiniones de los usuarios en negativas, neutrales y positivas. Por último, se desarrollará un sistema de recomendación que sugerirá juegos a los usuarios en base a sus preferencias y comportamientos anteriores. El proyecto demostrará nuestras habilidades en la implementación de soluciones en el campo de MLOps, desde la preparación de datos hasta el despliegue de modelos en producción.

### Datos 
El proyecto utiliza tres bases de datos distintas:
* **[australian_user_reviews.json:](datasets/australian_user_reviews.rar)** es un dataframe que contiene los comentarios que los usuarios realizaron sobre los juegos que utilizan , recomendaciones o no de ese juego; además de datos como url y user_id.s 
* **[australian_users_items.json:](datasets/australian_users_items.rar)** es un dataframe que contiene información sobre cada juego que utilizan los usuarios, y el tiempo que cada usuario jugo.
* **[output_steam_games.json:](datasets/output_steam_games.rar)** es un dataframe que contiene los comentarios que los usuarios realizaron sobre los juegos que utilizan , recomendaciones o no de ese juego; además de datos como url y user_id.

# Proceso del proyecto
 **ETL: Extracción, transformación y Carga de datos**

Se realizó un trabajo de transformación de datos tomando los archivos JSON, desanidando columnas, haciendo una limpieza de datos removiendo valores nulos y duplicados, creando columnas importantes para el futuro analisis, como el analisis de sentimiento, y así preparar los datos que vamos a utilizar en los endpoints.

Proceso de ETL por dataframe:
* **[ETL_games](ETL_games.ipynb)**
* **[ETL_items](ETL_items.ipynb)**
* **[ETL_reviews](ETL_reviews.ipynb)**

**EDA: Análisis Exploratorio de Datos**

Una vez finalizado el ETL, se hizo un analisis de las tres bases de datos generando gráficos y análisis que proporcionan una comprensión rápida de las tendencias clave. Exploramos usuarios, reseñas y características del juego para un análisis completo. Pueden revisar el EDA [aquí](/EDA.ipynb)

**Desarrollo de la API**

Para la creacion y posterior deployment de nuestra API, empezamos creando las funciones que se van a utilizar en la misma:

* **`developer`**: función para obtener información sobre la cantidad de juegos y porcentaje de juegos gratuitos por año de un desarrollador especifico.
* **`userdata`**: esta función calcula la cantidad de dinero gastado por un usuario en juegos, el porcentaje de recomendación y la cantidad de items que ha adquirido.
* **`UserForGenre`**: esta función busca al usuario que ha acumulado más horas jugadas en un género específico y devuelve una lista de la acumulación de horas jugadas por año de lanzamiento para ese género.
* **`best_developer_year`**: esta función devuelve el top 3 de desarrolladores con juegos más recomendados por usuarios para el año dado.
* **`developer_reviews_analysis`**: analiza las revisiones de los usuarios para un desarrollador específico y devuelve un diccionario con el nombre del desarrollador como clave y una lista con la cantidad total de registros de reseñas de usuarios clasificados como positivos o negativos.
* **`recomendacion_juego`**: Función que devuelve las recomendaciones de juegos para un juego específico según su ID. El modelado se generó en este notebook, que resultó en el dataframe utilizado por la misma función.

**FastAPI**

El código para generar la API se encuentra en el archivo [Main](/main.py). En caso de querer ejecutar la API desde localHost se deben seguir los siguientes pasos:

- Clonar el proyecto haciendo `git clone https://github.com/jrabuffetti/ML_OPS_Steam/.git`.
- Preparación del entorno de trabajo en Visual Studio Code:
    * Crear entorno `python -m venv env`
    * Ingresar al entorno haciendo `env\Scripts\activate`
    * Instalar dependencias con `pip install -r requirements.txt`
- Ejecutar el archivo `main.py` desde consola activando uvicorn. Para ello, hacer `uvicorn main:app --reload`
- Hacer Ctrl + clic sobre la dirección `http://XXX.X.X.X:XXXX` (se muestra en la consola).
- Una vez en el navegador, agregar `/docs` para acceder a ReDoc.
- En cada una de las funciones hacer clic en *Try it out* y luego introducir el dato que requiera o utilizar los ejemplos por defecto. Finalmente Ejecutar y observar la respuesta.

**Deploy**

El deploy de la API se hizo en render, que es una plataforma que se utiliza para crear y ejecutar aplicaciones o sitios web.
Pueden ingresar al deploy de la API entrando **[aquí](https://pi-ml-0c6n.onrender.com/docs)**

**Video descriptivo**

En [este video](https://www.youtube.com/watch?v=MXpJKYfn17M) se muestran las funciones, el uso de FastAPI y deploy en Render

**Conclusiones**

Durante la ejecución de este proyecto, se aplicaron los conocimientos adquiridos a lo largo del programa de Data Science en HENRY. Las tareas realizadas abarcaron tanto las responsabilidades típicas de un Data Engineer como las de un Data Scientist.

Hemos alcanzado con éxito el objetivo de desarrollar un Producto Mínimo Viable (MVP), que incluye una API y su despliegue en un servicio web. Se logró un buen nivel de optimización teniendo en cuenta las limitaciones de almacenamiento.

## Autor ✒️
* **Juan Rabuffetti**  - [LinkedIn](https://www.linkedin.com/in/juan-rabuffetti/)
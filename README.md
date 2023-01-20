<p align='left'><img src='https://assets.ubuntu.com/v1/ad9a02ac-ubuntu-orange.gif'height=70><p>

<p align='center'><img src='https://www.python.org/static/community_logos/python-logo.png' height=70><p>
<p align='right'><img src='https://code.visualstudio.com/assets/images/code-stable.png' height=70 ><p>


# <h1 align='center'>**`Data Engineering`**</h1>
# <h2 align='center'>**`FastAPI deployed in Deta`**</h2>


<p align="center"><img src="https://files.realpython.com/media/What-is-Data-Engineering_Watermarked.607e761a3c0e.jpg"  height=300></p>

<hr>  

## Contexto

`Application Programming Interface`  es una interfaz que permite que dos aplicaciones se comuniquen entre sí, independientemente de la infraestructura subyacente. Son herramientas muy versátiles que permiten por ejemplo, crear pipelines facilitando mover y brindar acceso simple a los datos que se quieran disponibilizar a través de los diferentes endpoints, o puntos de salida de la API.

Hoy en día contamos con **FastAPI**, un web framework moderno y de alto rendimiento para construir APIs con Python.
<p align=center>
<img src = 'https://i.ibb.co/9t3dD7D/blog-zenvia-imagens-3.png' height=250><p>

## Rol a desarrollar

Como parte del equipo de data de una empresa, el área de análisis de datos le solicita al área de Data Engineering (usted) ciertos requerimientos para el óptimo desarrollo de sus actividades. Usted deberá elaborar las *transformaciones* requeridas y disponibilizar los datos mediante la *elaboración y ejecución de una API*.



## **Propuesta de trabajo (requerimientos)**


**`Transformaciones`**:  El analista de datos requiere estas, ***y solo estas***, transformaciones para sus datos:


+ Generar campo **`id`**: Cada id se compondrá de la primera letra del nombre de la plataforma, seguido del show_id ya presente en los datasets (ejemplo para títulos de Amazon = **`as123`**)

+ Los valores nulos del campo rating deberán reemplazarse por el string “**`G`**” (corresponde al maturity rating: “general for all audiences”

+ De haber fechas, deberán tener el formato **`AAAA-mm-dd`**

+ Los campos de texto deberán estar en **minúsculas**, sin excepciones

+ El campo ***duration*** debe convertirse en dos campos: **`duration_int`** y **`duration_type`**. El primero será un integer y el segundo un string indicando la unidad de medición de duración: min (minutos) o season (temporadas)


<br/>

**`Desarrollo API`**:  Para disponibilizar los datos la empresa usa el framework ***FastAPI***. 

Las funciones utilizadas para realizar las consultas, se encuentran en el archivo [main.py](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/PI_01/main.py), dentro de la carpeta PI_01 (carpeta con los archivos necesarios para el deployment de la app a través de [deta](https://www.deta.sh/). 

El analista de datos requiere consultar:

+ Cantidad de veces que aparece una keyword en el título de peliculas/series, por plataforma

+ Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año

+ La segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos.

+ Película que más duró según año, plataforma y tipo de duración

+ Cantidad de series y películas por rating
<br/>

# <h2 align='left'>**`DESARROLLO DEL PROJECTO`**</h2>
<br/>

El desarrollo del projecto consta esencialmente de 3 pasos:

**`1. Desarrollo de la API`**:
- 1.- Deta
  - Creación de Projecto en Deta
  - Creación de Micros en Deta
- 2.- FastAPI
- Instalación
  - Creación del archivo *main.py*
  - Creación del archivo *requirements.txt*

**`2. Carga de los datasets (Transformaciones)`**
- 1.- Carga de los datasets
- 2.- Fase de transformación
- 3.- Guardado en directorio de deta para utilizarlo en el deploy

**`3. Testeo de las querys`**
- 1.- Ejecución de las querys definidas en el archivo *main.py* de FastAPI.

# <h3 align='center'>**`1. Desarrollo de la API`**</h3>


- 1.- Deta
  - Creación de Projecto en Deta. A través del sitio web en la siguiente opción (el nombre del projectó sera HENRY): 
  <p align="center"><img src="/home/osanchezd/Documents/HENRY/dataft06/PI01-Data-Engineering/_src/Screenshot from 2023-01-20 06-33-30.png"  height=300></p>
  
  - Creación de Micros en Deta. Se realiza, utilizando la siguiente línea de código en CLI:
  ```python
  deta new "my-micro" --project HENRY
  ```
  Para este caso en particular "my_micro" = PI_01.

  Al ejecutarse el código anterior, se creará una carpeta con el nombre "my_micro". Más adelante, dentro de esa carpeta, colocaremos los datasets y además, crearemos el archivo *[requirements.txt](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/PI_01/requirements.txt)*, en la cual indicaremos las dependencias necesarias para desplegar FastAPI en Deta.

  Para desplegar los cambios (una vez que se hayan realizado), se ejecuta la siguiente línea de código en CLI:
  ```python
  deta deploy
  ```

  Una vez ejecutados los pasos anteriores, procedemos a instalar, y a la creación de los archivos requeridos por FastAPI para desplegar la app en Deta.

- 2.- FastAPI
- Instalación. Se realiza mediante el comando:
```python
pip install fastapi
```
  - Creación del archivo *[main.py](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/PI_01/main.py)*. En este archivo, se definen las funciones, que nos ayudarán a ejecutar las querys deseadas.
  
  - Creación del archivo [requirements.txt](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/PI_01/requirements.txt). Aquí, se indican las dependencias necesarias para desplegar la app.
```python
# Contenido del archivo `requirements.txt`
fastapi
pandas
```

*`2. Carga de los datasets (Transformaciones)`*

```python
# Importamos las librerias requeridas
import pandas as pd
import re
import os
from ETL_Functions.etl_functions import load_csv, etl, concat_df
```

Las funciones `load_csv`, `etl` y `concat_df`, se encuentran definidas en el archivo [etl.functions.py](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/ETL_Functions/etl_functions.py)

```python
# Obtenemos la lista de todos los archivos .csv en nuestro directorio `Datasets`
path = '../PI01-Data-Engineering/Datasets/'
dir_list = os.listdir(path)

print(f'Files stored in {path}:')
for files in dir_list:  
    print(files)  # imprimimos todos los archivos

# Hacemos uso de nuestra primera función para cargar los archivos .csv
# Cargando los archivos .csv
for file in dir_list:
    if re.search(r'.csv', file):
        load_csv(path, file)
```
La función [load_csv](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/ETL_Functions/etl_functions.py#:~:text=def%20load_csv(path%2C%20file)%3A), guardará los archivos cargados en al ruta `../PI01-Data-Engineering/Datasets_for_ETL/`, para su carga en la siguient fase (ETL).
<br/>

## 2. Fase de transformación (ETL)
```python
# Definimos la ruta de búsqueda de nuestros archivos previamente cargados
# de la base original, para ser transformados posteriormente
path_ETL = '../PI01-Data-Engineering/Datasets_for_ETL/'
dir_list_ETL = os.listdir(path_ETL)

# Obtenemos la lista de todos los archivos .csv en nuestro directorio `Datasets_for_ETL
print(f'Files stored in {path_ETL}:')
for files_etl in dir_list_ETL:  # imprime todo los archivos
    print(files_etl)
````

Con ayuda de la función [etl](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/ETL_Functions/etl_functions.py#:~:text=def%20etl(path_ETL%2C%20file_ETL)) aplicamos las siguientes transformaciones:

*1.- Generar campo **`id`**: Cada id se compondrá de la primera letra del nombre de la plataforma, seguido del show_id ya presente en los datasets (ejemplo para títulos de Amazon = **`as123`**)*
```python
# Ejemplo:
# Parte del código de la funcion `etl` para la generación del `id`
for files_etl in dir_list_ETL:
        if re.search(r'amazon', files_etl):
            amazon = pd.read_csv(path_ETL+files_etl)
            amazon['id'] = 'a' + amazon['show_id']
```

*2.- Los valores nulos del campo rating deberán reemplazarse por el string “**`G`**” (corresponde al maturity rating: “general for all audiences”*
```python
# Ejemplo:
# Parte del código de la funcion `etl` para el reemplazo de valores nulos por `G`
amazon['rating'].fillna('G', inplace=True)
```

*3.- De haber fechas, deberán tener el formato **`AAAA-mm-dd`***
```python
# Ejemplo:
# Parte del código de la funcion `etl` para el formato de la fecha
amazon['date'] = pd.to_datetime(amazon['date_added'])
```

*4.- Los campos de texto deberán estar en **minúsculas**, sin excepciones*
```python
# Ejemplo:
# Parte del código de la funcion `etl` para modificar los campos de texto a minúsculas.
for i in amazon.select_dtypes(include='object'):
                amazon[i] = amazon[i].str.lower()
```

*5.- El campo ***duration*** debe convertirse en dos campos: **`duration_int`** y **`duration_type`**. El primero será un integer y el segundo un string indicando la unidad de medición de duración: min (minutos) o season (temporadas)*
```python
# Ejemplo:
# Parte del código de la funcion `etl` para modificar el campo `duration` a `duration_int` y `duration_type`
duration = amazon['duration'].str.split(" ", n=1, expand=True)
amazon['duration_int'] = duration[0]
amazon['duration_type'] = duration[1]
amazon['duration_type'] = amazon['duration_type'].str.replace('seasons', 'season')
amazon['duration_int'] = amazon['duration_int'].astype(int)
```

Asi mismo, se homologó `seasons` a `season` en el campo `duration_type`.

Una vez realizado el proceso de ETL, con los requerimientos solicitados, procedemos a concatenar los 4 dataframes tratados **(`amazon`, `hulu`, `disney`, `netflix`)**. 

Para efectuar esta tarea, hacemos uso de la función [concat_df](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/ETL_Functions/etl_functions.py#:~:text=def%20concat_df(path%2C%20dir)%3A).

```python
# Listamos los dataframes tratados
path_df_clean = '../PI01-Data-Engineering/PI_01/'
dir_list_clean = os.listdir(path_df_clean)

concat_df(path_df_clean, dir_list_clean)
```





<p align='left'><img src='https://assets.ubuntu.com/v1/ad9a02ac-ubuntu-orange.gif'height=75><p>

<p align='center'><img src='https://www.python.org/static/community_logos/python-logo.png' height=75><p>

<p align='right'><img src='https://code.visualstudio.com/assets/images/code-stable.png' height=75 ><p>

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

Las transformaciones se realizaron utilizando la funcion __[**etl(path_ETL, file_ETL)**](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/ETL_Functions/etl_functions.py)__

<br/>

**`Desarrollo API`**:  Para disponibilizar los datos la empresa usa el framework ***FastAPI***. 

Las funciones utilizadas para realizar las consultas, se encuentran en el archivo [main.py](https://github.com/Oscarszdz/PI01-Data-Engineering/blob/main/PI_01/main.py), dentro de la carpeta PI_01 (carpeta con los archivos necesarios para el deployment de la app a través de [deta](https://www.deta.sh/). 

El analista de datos requiere consultar:

+ Cantidad de veces que aparece una keyword en el título de peliculas/series, por plataforma
```python
# Number of times a keyword appears in movies/series Title's, by platform
# 1.- get_word_count('netflix', 'love')
@app.get("/word_count/{platform}/{word}")
async def read_item(platform: str, word: str):
    if platform in ['netflix', 'hulu', 'disney', 'amazon']:
        df = pd.read_csv(platform+'.csv')
        count = df['title'].str.count(word).sum()
        response = dict(platform=platform, cantidad=str(count))
        return response
    else:
        # return {'platform not available': platform}
        return {f'Platform not available: {platform}. Try again.'}
```

+ Cantidad de películas por plataforma con un puntaje mayor a XX en determinado año

+ La segunda película con mayor score para una plataforma determinada, según el orden alfabético de los títulos.

+ Película que más duró según año, plataforma y tipo de duración

+ Cantidad de series y películas por rating
<br/>



## **Criterios de evaluación**

**`Código`**: Prolijidad de código, uso de clases y/o funciones, en caso de ser necesario, código comentado. 

**`Repositorio`**: Nombres de archivo adecuados, uso de carpetas para ordenar los archivos, README.md presentando el proyecto y el trabajo realizado

**`Cumplimiento`** de los requerimientos de aprobación indicados en el apartado `Propuesta de trabajo`

NOTA: Recuerde entregar el link de acceso al video. Puede alojarse en YouTube, Drive o cualquier plataforma de almacenamiento. **Verificar que sea de acceso público**.

<br/>

## **Fuente de datos**

+ Podrán encontrar los archivos con datos en la carpeta Datasets, en este mismo repositorio.<sup>*</sup>
<br/>

## **Material de apoyo**

Imagen Docker con Uvicorn/Guinicorn para aplicaciones web de alta performance:

+ https://hub.docker.com/r/tiangolo/uvicorn-gunicorn-fastapi/ 

+ https://github.com/tiangolo/uvicorn-gunicorn-fastapi-docker

FAST API Documentation:

+ https://fastapi.tiangolo.com/tutorial/

"Prolijidad" del codigo:

+ https://pandas.pydata.org/docs/development/contributing_docstring.html

<br/>

## **Deadlines importantes**

+ Apertura de formularios de entrega de proyectos: **Miercoles 18, 15:00hs gmt -3**

+ Cierre de formularios de entrega de proyectos: **Viernes 20, 12:00hs gmt-3**
  
+ Demo por parte del estudiante: **Viernes 20, 16:00hs gmt-3** 

(Se escogera entre l@s estudiantes aquel que represente de **forma global** todos los criterios de evaluacion esperados, para que sirva de inspiracion a sus compañer@s)

## `Disclaimer`
De parte del equipo de Henry se aclara y remarca que el fin de los proyectos propuestos es exclusivamente pedagógico, con el objetivo de realizar simular un entorno laboral, en el cual se trabajan diversas temáticas ajustadas a la realidad. No reflejan necesariamente la filosofía y valores de la organización. Además, Henry no alienta ni tampoco recomienda a los alumnos y/o cualquier persona leyendo los repositorios (y entregas de proyectos) que tomen acciones con base a los datos que pudieran o no haber recabado. Toda la información expuesta y resultados obtenidos en los proyectos nunca deben ser tomados en cuenta para la toma real de decisiones (especialmente en la temática de finanzas, salud, política, etc.).

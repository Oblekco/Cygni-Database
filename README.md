# Cygni-Database

Primero contamos con el archivo ```cygni_edomex_backup_20220427_1223.sql``` que guarda todos los datos cargados por Hector Aguirre.

Posteriormente se tiene el script en Python ```cygniEdomex.py ``` que realiza la conexion con ```MySql``` para poder extraer toda la informacion de la base de datos ```cygni_edomex``` y asi procesarla a traves de dataframes. Este script retorna un dataframe general llamado ```df``` que consta de 35 columnas vs 1.063.711 filas. 

Para tener un claro panorama del programa en python se puede recurrir al notebook ```cygniEdomex.ipynb``` en donde tenemos los siguientes valores agregados: Podemos generar archivos en ```javaScript``` en donde se inserta los datos del dataframe para poder tener tablas dinamicas. Los archivos en ```HTML``` son el resultado de dicho procedimiento. Ademas se genera un archivo ```csv``` para volverlo a cargar en una nueva base de datos de prueba.
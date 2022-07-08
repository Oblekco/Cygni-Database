# PROYECTO CYGNI

Se parte del siguiente archivo ```cygni_edomex_backup_20220427_1223.sql``` el cual contiene la base de datos ```cygni_edomex``` con las siguientes tablas:

```sql
+--------------------------------+
| Tables_in_cygni_edomex         |
+--------------------------------+
| con_indicadores_pobreza_c19    |
| con_indicadores_pobreza_c4     |
| con_indicadores_pobreza_c56    |
| con_indicadores_pobreza_c8     |
| con_medicion_pobreza_c18       |
| con_medicion_pobreza_c19       |
| con_medicion_pobreza_c4        |
| con_medicion_pobreza_c56       |
| con_medicion_pobreza_c8a       |
| con_medicion_pobreza_c8b       |
| entidades_federativas          |
| ine_condiciones_pobreza        |
| ine_ingreso_corriente_percap   |
| ine_materiales_piso            |
| ine_niveles_ingreso            |
| ine_poblacion_desempleo        |
| ine_poblacion_ingresos         |
| ine_poblacion_pobreza          |
| ine_poblacion_tipo_pobreza     |
| ine_razones_desempleo          |
| ine_tenencia_hogares_propiedad |
| ine_tipos_pobreza              |
| ine_tipos_tenencia_vivienda    |
| ine_viviendas_material_piso    |
| municipios                     |
| seg_bienes_juridicos           |
| seg_conceptos_delito_ff        |
| seg_delitos_c100m_habitantes   |
| seg_generos                    |
| seg_incidencia_delictiva       |
| seg_incidencia_ff              |
| seg_leyes_ff                   |
| seg_modalidades_delito         |
| seg_rangos_edad                |
| seg_subtipos_delito            |
| seg_tipos_delito               |
| seg_tipos_delito_ff            |
| seg_unidades_robadas           |
| seg_victimas_fuero_comun       |
+--------------------------------+
```

Las fuentes de los datos provienen de tres bases específicamente.

### Listado de bases de datos SESNSP

| N | Nombre base                         | Ámbito        | Nivel             | Fuente | Periodo                                      |
|---|-------------------------------------|---------------|-------------------|--------|----------------------------------------------|
| 1 | Incidencia delictiva                | Fuero Común   | Estatal/Municipal | SESNSP | 2015 - 2020 completos 2021 - hasta noviembre |
| 2 | Incidencia fuero federal            | Fuero federal | Estatal           | SESNSP | 2015 - 2020 completos 2021 - hasta noviembre |
| 3 | Victimas del fuero común            | Fuero común   | Estatal           | SESNSP | 2015 - 2020 completos 2021 - hasta noviembre |
| 4 | Unidades robadas                    | Fuero común   | Estatal           | SESNSP | 2015 - 2020 completos 2021 - hasta noviembre |
| 5 | Delitos por cada 100 mil habitantes | Fuero común   | Estatal           | SESNSP | 2015 - 2020 completos 2021 - hasta noviembre |


### Listado de bases de datos CONEVAL

| N° | Nombre base                                     | Indicador                                                                           | Nivel                           | Fuente  | Periodo        |
|----|-------------------------------------------------|-------------------------------------------------------------------------------------|---------------------------------|---------|----------------|
| 1  | MEDICIÓN DE POBREZA                             | POBREZA                                                                             | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | CARENCIAS PROMEDIO POR INDICADOR DE POBREZA                                         | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | INDICADORES DE CARENCIA SOCIAL                                                      | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | INDICADORES DE CARENCIA SOCIAL                                                      | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | MEDIDAS DE PROFUNDIDAD E INTENSIDAD DE LA POBREZA,                                  | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | MEDIDAS DE PROFUNDIDAD E INTENSIDAD DE LA POBREZA                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | COEFICIENTE DE GINI                                                                 | ESTATAL                         | CONEVAL | 2008 - 2014    |
|    | MEDICIÓN DE POBREZA                             | COEFICIENTE DE GINI                                                                 | ESTATAL                         | CONEVAL | 2016 – 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 a 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | COMPONENTES DE LOS INDICADORES DE CARENCIA SOCIAL                                   | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | CARENCIAS PROMEDIO POR INDICADOR (MÉXICO)                                           | ESTATAL                         | CONEVAL | 2008 - 2018    |
|    | MEDICIÓN DE POBREZA                             | EVOLUCIÓN DE LA POBREZA Y POBREZA EXTREMA                                           | ESTATAL                         | CONEVAL | 2008 - 2018    |
| 2  | MEDICIÓN DE POBREZA 2                           | COEFICIENTE DE GIN                                                                  | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | CARENCIAS PROMEDIO POR INDICADOR DE POBREZA                                         | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | CARENCIAS PROMEDIO POR INDICADOR DE POBREZA                                         | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | INDICADORES DE CARENCIA SOCIAL                                                      | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | INDICADORES DE CARENCIA SOCIAL                                                      | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | MEDIDAS DE PROFUNDIDAD E INTENSIDAD DE LA POBREZA                                   | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | MEDIDAS DE PROFUNDIDAD E INTENSIDAD DE LA POBREZA                                   | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | INGRESO CORRIENTE TOTAL PER CÁPITA (PRECIOS AGOSTO 2020)                            | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE REZAGO EDUCATIVO                                                     | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE CARENCIA POR ACCESO A LOS SERVICIOS DE SALUD                         | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE CARENCIA POR ACCESO A LA SEGURIDAD SOCIAL                            | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE CARENCIA POR CALIDAD Y ESPACIOS DE LA VIVIENDA                       | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE CARENCIA POR ACCESO A LOS SERVICIOS BÁSICOS EN LA VIVIENDA           | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | COMPONENTES DE CARENCIA POR ACCESO A LA ALIMENTACIÓN NUTRITIVA Y DE CALIDAD         | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | CARENCIAS PROMEDIO DE LA POBLACIÓN EN SITUACIÓN DE POBREZA POR GRUPOS ETARIOS,      | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | CARENCIAS PROMEDIO DE LA POBLACIÓN EN SITUACIÓN DE POBREZA POR ÁMBITO DE RESIDENCIA | ESTATAL                         | CONEVAL | 2016 - 2020    |
|    | MEDICIÓN DE POBREZA 2                           | EVOLUCIÓN DE LA POBREZA                                                             | ESTATAL                         | CONEVAL | 2016 - 2020    |
| 3  | POBREZA A NIVEL MUNICIPIO                       | CONCENTRADO MUNICIPAL                                                               | MUNICIPAL                       | CONEVAL | 2010 - 2020    |
|    | POBREZA A NIVEL MUNICIPIO                       | CONCENTRADO ESTATAL                                                                 | ESTATAL                         | CONEVAL | 2010 - 2020    |
| 4  | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN CON CARENCIA POR ACCESO A LOS SERVICIOS DE SALUD                          | ESTATAL                         | CONEVAL | 2010 - 2020    |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN CON REZAGO EDUCATIVO                                                      | ESTATAL                         | CONEVAL | 1990-2020      |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS  CON CARENCIA POR MATERIAL DE PISOS                          | ESTATAL                         | CONEVAL | 1990 - 2020    |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR MATERIAL DE MUROS                           | ESTATAL                         | CONEVAL | 1990 –2020     |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR MATERIAL DE TECHOS                          | ESTATAL                         | CONEVAL | 1990 –2020     |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR HACINAMIENTO                                | ESTATAL                         | CONEVAL | 1990 –2020     |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR ACCESO AL AGUA ENTUBADA                     | ESTATAL                         | CONEVAL | 1990 –2020     |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR SERVICIO DE DRENAJE                         | ESTATAL                         | CONEVAL | 1990 –2020     |
|    | EVOLUCIÓN DE LAS DIMENSIONES DE LA POBREZA      | POBLACIÓN EN VIVIENDAS CON CARENCIA POR SERVICIO DE ELECTRICIDAD                    | ESTATAL                         | CONEVAL | 1990 –2020     |
| 5  | POBREZA LABORAL TERCER TRIMESTRE 2021           | ÍNDICE DE LA TENDENCIA LABORAL DE LA POBREZA (ITLP)                                 | ESTATAL                         | CONEVAL | De 2005 a 2021 |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | INGRESO LABORAL PER CÁPITA                                                          | ESTATAL                         | CONEVAL | De 2005 a 2021 |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | MASA SALARIAL                                                                       | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | ÍNDICE DE LA TENDENCIA LABORAL DE LA POBREZA                                        | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | POBLACIÓN CON INGRESO LABORAL INFERIOR AL COSTO DE LA CANASTA ALIMENTARIA           | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | INGRESO LABORAL PER CÁPITA                                                          | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | MASA SALARIAL                                                                       | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | POBLACIÓN CON INGRESO LABORAL INFERIOR AL COSTO DE LA CANASTA ALIMENTARIA           | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL TERCER TRIMESTRE 2021           | INGRESO LABORAL PROMEDIO EN LA POBLACIÓN OCUPADA SEGÚN SEXO Y ENTIDAD FEDERATIVA    | ESTATAL                         | CONEVAL | 2005 a 2021    |
| 6  | POBREZA LABORAL INDICADORES A CORTO PLAZO       | POBLACIÓN CON INGRESO LABORAL INFERIOR AL COSTO DE LA CANASTA ALIMENTARIA (         | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL INDICADORES A CORTO PLAZO       | INDICADOR TRIMESTRAL DE LA ACTIVIDAD ECONÓMICA ESTATAL (ITAEE)                      | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL INDICADORES A CORTO PLAZO       | SALARIO BASE DE COTIZACIÓN DEL IMSS                                                 | ESTATAL                         | CONEVAL | 2005 a 2021    |
|    | POBREZA LABORAL INDICADORES A CORTO PLAZO       | TASA DE INFORMALIDAD NACIONAL SEGÚN SEXO Y ENTIDAD FEDERATIVA                       | ESTATAL                         | CONEVAL | 2005 a 2021    |
| 7  | ÍNDICE DE REZAGO SOCIAL                         | GRADO DE REZAGO SOCIAL                                                              | ESTATAL / MUNICIPAL             | CONEVAL | 2020           |
|    | ÍNDICE DE REZAGO SOCIAL                         | GRADO DE REZAGO SOCIAL                                                              | ESTATAL / MUNICIPAL             | CONEVAL | 2015           |
|    | ÍNDICE DE REZAGO SOCIAL                         | GRADO DE REZAGO SOCIAL                                                              | ESTATAL / MUNICIPAL             | CONEVAL | 2010           |
| 8  | ÍNDICE DE REZAGO SOCIAL LOCALIDAD               | GRADO DE REZAGO SOCIAL                                                              | LOCALIDAD                       | CONEVAL | 2010           |
|    | ÍNDICE DE REZAGO SOCIAL LOCALIDAD               | GRADO DE REZAGO SOCIAL                                                              | LOCALIDAD                       | CONEVAL | 2020           |
| 9  | GRADO DE ACCESIBILIDAD A CARRETERA PAVIMENTADA  | ACCESIBILIDAD CARRETERA PAVIMENTADA                                                 | ESTATAL / MUNICIPAL / LOCALIDAD | CONEVAL | 2020           |
| 10 | COHESIÓN SOCIAL                                 | COHESIÓN SOCIAL                                                                     | ESTATAL                         | CONEVAL | 2008 - 2020    |

### Listado de bases de datos INEGI

| N° | Nombre base               | Indicador                                   | Nivel             | Fuente | Periodo                |
|----|---------------------------|---------------------------------------------|-------------------|--------|------------------------|
| 1  | INDICADORES POBREZA INEGI | POBLACIÓN EN CONDICION DE POBREZA           | ESTATAL           | INEGI  | 2018 y 2020 completos  |
|    | INDICADORES POBREZA INEGI | POBLACIÓN POR TIPO DE POBREZA               | ESTATAL           | INEGI  | 2018 y 2020 completos  |
|    | INDICADORES POBREZA INEGI | POBLACIÓN POR NIVEL DE INGRESOS             | ESTATAL           | INEGI  | 2015 y 2021 trimestral |
|    | INDICADORES POBREZA INEGI | INGRESOS CORRIENTES POR DECIL               | ESTATAL           | INEGI  | 2016 – 2020 completos  |
|    | INDICADORES POBREZA INEGI | POBLACIÓN DESOCUPARA POR RAZON DE DESEMPLEO | ESTATAL           | INEGI  | 2015 – 2021 trimestral |
|    | INDICADORES POBREZA INEGI | VIVIENDAS POR MATERIAL DE PISO              | ESTATAL/MUNICIPAL | INEGI  | 2010 Y 2020 completos  |
|    | INDICADORES POBREZA INEGI | DISPONIBILIDAD DE SERVICIOS                 | ESTATAL           | INEGI  | 2020 – 2010 completos  |
|    | INDICADORES POBREZA INEGI | TENENCIA DE VIVIENDA POR PROPIEDAD          | ESTATAL           | INEGI  | 2018 y 2020 completos  |
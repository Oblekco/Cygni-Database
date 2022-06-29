#!/usr/bin/env python
# coding: utf-8

import mysql.connector
import pandas as pd
from collections import OrderedDict



def generacionDF(host, user, passwd, database):
    mi_conexion = mysql.connector.connect(host = host, 
                                            user = user, 
                                            passwd = passwd, 
                                            database = database)
    mi_cursor = mi_conexion.cursor()


    mi_cursor.execute("SHOW TABLES")
    resultado = mi_cursor.fetchall() 
        
    lista = []
    for i in resultado:
        lista.append(i[0])
        
    desc = 'DESC '
    listanueva = []
    for i in range(len(lista)):
        tabla = lista[i]
        argumento = desc + tabla
        listanueva.append(argumento)
        
    descripciones = []
    for i in range(len(listanueva)):
        mi_cursor = mi_conexion.cursor()
        mi_cursor.execute(listanueva[i])
        resultado = mi_cursor.fetchall()
        descripciones.append(resultado)
        mi_cursor.close()

    mi_cursor.close()


    selectFrom = 'SELECT * FROM '
    argumentos2 = []
        
    for i in range(len(listanueva)):
        tabla2 = lista[i]
        argumento2 = selectFrom + tabla2
        argumentos2.append(argumento2)
        
    registros = []
    
    for i in range(0,len(argumentos2)):
        mi_cursor = mi_conexion.cursor()
        mi_cursor.execute(argumentos2[i])
        resultado = mi_cursor.fetchall()
        registros.append(resultado)
        mi_cursor.close()



    def dataframe_tabla(n):
        des = descripciones[n]
        reg = registros[n]
        # Extraemos los datos
        campos = []
        for i in des:
            campos.append(i[0])
                
        valores = []
            
        for i in reg:
            y = list(i)
            valores.append(y)
                
        x = 1
    
        datos = []
            
        while x <= len(campos):
            for i in range(len(reg)):
                z = valores[i][x-1]
                datos.append(z)
            x += 1
        
        dicc = {}
        a = 0
        b = len(reg)
        
        for j in range(len(campos)):
            d = {campos[j]:datos[a:b]}
            dicc.update(d)
            a += len(reg)
            b += len(reg)
    
        df = pd.DataFrame(dicc)
        return df
    
    listaData = []
    for i in range(len(listanueva)):
        df = dataframe_tabla(i)
        listaData.append(df)
    
    
    mapa1 = {}
    for i in range(len(listanueva)):
        mapa = {lista[i]: listaData[i]}
        mapa1.update(mapa)
    
    mi_conexion.close()
   
    return mapa1


def procesamientoDF(host, user, passwd, database):
    global df1, df2, df3, df4, df5, df6, df7, df8, df9,df10
    global df11, df12, df13, df14, df15, df16, df17, df18, df19,df20
    global df21, df22, df23, df24, df25, df26, df27, df28, df29,df30
    global df31, df32, df33, df34, df35, df36, df37, df38, df39
    global list_difference
    
    mapa1 = generacionDF(host, user, passwd, database)
    listKeys = []
    for keys in mapa1:
        listKeys.append(keys)
        
    for i in range(1,len(listKeys)+1):
        globals()["df" + str(i)] = mapa1[listKeys[i-1]]
    
    
    df2_new = df2.rename(columns={'id': 'id_indicador'})
    merge01 = pd.merge(df2_new, df7, on='id_indicador')
    df11_new = df11.rename(columns = {'id': 'id_entidad'})
    merge01_new = pd.merge(merge01, df11_new, on = 'id_entidad')
    
    df1_new = df1.rename(columns={'id':'id_indicador'})
    merge02 = pd.merge(df1_new, df6, on='id_indicador')
    merge02_new = pd.merge(merge02, df11_new, on='id_entidad')
    
    df3_new = df3.rename(columns={'id':'id_indicador'})
    merge03 = pd.merge(df3_new, df8, on='id_indicador')
    merge03_new = pd.merge(merge03, df11_new, on='id_entidad')
    
    df4_new = df4.rename(columns = {'id':'id_indicador'})
    
    merge04_a = pd.merge(df4_new, df9, on='id_indicador')
    merge04_a['porcentaje'] = pd.Series([],dtype=pd.StringDtype())
    merge04_a_new = merge04_a[['id_indicador', 'nombre', 
                               'activo', 'id', 'id_entidad', 
                               'anio', 'porcentaje', 'profundidad', 'intensidad']]
        
    merge04_b = pd.merge(df4_new, df10, on='id_indicador')
    merge04_b['profundidad'] = pd.Series([],dtype=pd.StringDtype())
    merge04_b['intensidad'] = pd.Series([],dtype=pd.StringDtype()) 
    merge04concat = pd.concat([merge04_a_new, merge04_b])
    merge04 = pd.merge(merge04concat, df11_new, on = 'id_entidad')
     
    
    claves = []
    for i in merge01_new.keys():
        claves.append(i)
    for i in merge02_new.keys():
        claves.append(i)
    for i in merge03_new.keys():
        claves.append(i)
    for i in merge04.keys():
        claves.append(i)
    
    final_list = list(OrderedDict.fromkeys(claves))
    
    
    difference_1 = set(final_list).difference(set(merge01_new))
    difference_2 = set(merge01_new).difference(set(final_list))
        
    list_difference = list(difference_1.union(difference_2))
        
    merge01_new['id_padre'] = pd.Series([],dtype=pd.StringDtype())
    merge01_new['poblacion'] = pd.Series([],dtype=pd.StringDtype())
    merge01_new['profundidad'] = pd.Series([],dtype=pd.StringDtype())
    merge01_new['intensidad'] = pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list).difference(set(merge02_new))
    difference_2 = set(merge02_new).difference(set(final_list))
        
    list_difference = list(difference_1.union(difference_2))
        
    merge02_new['intensidad'] = pd.Series([],dtype=pd.StringDtype())
    merge02_new['poblacion'] = pd.Series([],dtype=pd.StringDtype())
    merge02_new['profundidad'] = pd.Series([],dtype=pd.StringDtype())
    merge02_new['promedio'] = pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list).difference(set(merge03_new))
    difference_2 = set(merge03_new).difference(set(final_list))
        
    list_difference = list(difference_1.union(difference_2))
        
    merge03_new['intensidad'] = pd.Series([],dtype=pd.StringDtype())
    merge03_new['id_padre'] = pd.Series([],dtype=pd.StringDtype())
    merge03_new['profundidad'] = pd.Series([],dtype=pd.StringDtype())
    merge03_new['promedio'] = pd.Series([],dtype=pd.StringDtype())
    
    difference_1 = set(final_list).difference(set(merge04))
    difference_2 = set(merge04).difference(set(final_list))
       
        
    list_difference = list(difference_1.union(difference_2))
        
    merge04['personas'] = pd.Series([],dtype=pd.StringDtype())
    merge04['id_padre'] = pd.Series([],dtype=pd.StringDtype())
    merge04['poblacion'] = pd.Series([],dtype=pd.StringDtype())
    merge04['promedio'] = pd.Series([],dtype=pd.StringDtype())
    
    
    
    merge01_new = merge01_new[['id_indicador', 'nombre_x', 'activo_x', 
                               'id', 'id_entidad', 'anio', 'porcentaje', 
                               'personas', 'promedio', 'clave', 'nombre_y', 
                               'capital', 'activo_y', 'id_padre', 'poblacion', 
                               'profundidad', 'intensidad']]
    merge02_new = merge02_new[['id_indicador', 'nombre_x', 'activo_x', 
                               'id', 'id_entidad', 'anio', 'porcentaje', 
                               'personas', 'promedio', 'clave', 'nombre_y', 
                               'capital', 'activo_y', 'id_padre', 'poblacion', 
                               'profundidad', 'intensidad']]
    merge03_new = merge03_new[['id_indicador', 'nombre_x', 'activo_x', 
                               'id', 'id_entidad', 'anio', 'porcentaje', 
                               'personas', 'promedio', 'clave', 'nombre_y', 
                               'capital', 'activo_y', 'id_padre', 'poblacion', 
                               'profundidad', 'intensidad']]
    merge04 = merge04[['id_indicador', 'nombre_x', 'activo_x', 'id', 'id_entidad', 
                       'anio', 'porcentaje', 'personas', 'promedio', 'clave', 
                       'nombre_y', 'capital', 'activo_y', 'id_padre', 'poblacion', 
                       'profundidad', 'intensidad']]
        
    
    Coneval = pd.concat([merge01_new,merge02_new,merge03_new,merge04])
        
    
    c1 = Coneval.rename(columns={'nombre_x': 'indicador_pobreza'})
    c1_01 = c1.drop(['id_indicador'], axis=1)
    c1_02 = c1_01.rename(columns={'nombre_y': 'entidad'})
    c1_03 = c1_02.drop(['activo_x','id', 'clave','activo_y','id_padre'], axis=1)
    
    
    Coneval_final = c1_03
    
    
    result = []
    for item in Coneval_final.indicador_pobreza:
        if item not in result:
            result.append(item)
    
    
    
    df12_new = df12.rename(columns={'id': 'id_condicion'})
    merge05 = pd.merge(df12_new, df18, on='id_condicion')
    
    df15_new = df15.rename(columns={'id': 'id_nivel'})
    merge06 = pd.merge(df15_new, df17, on='id_nivel')
    
    df20_new = df20.rename(columns={'id': 'id_razon'})
    merge07 = pd.merge(df20_new, df16, on='id_razon')
    
    df14_new = df14.rename(columns={'id': 'id_material'})
    merge08 = pd.merge(df14_new, df24, on='id_material')
    
    df22_new = df22.rename(columns={'id': 'id_tipo'})
    merge09 = pd.merge(df22_new, df19, on='id_tipo')
    
    df23_new = df23.rename(columns={'id': 'id_tipo_tenecia'})
    merge10 = pd.merge(df23_new, df21, on='id_tipo_tenecia')
    
    df25_new = df25.rename(columns={'id': 'id_municipio'})
    merge11 = pd.merge(df25_new, merge08, on='id_municipio')
    
    
    
    merge05_01 = merge05.rename(columns={'nombre': 'condicion'})
    merge05_02= merge05_01.drop(['id_condicion','activo', 'id'], axis=1)
    
    merge06_01 = merge06.rename(columns={'nombre': 'nivel'})
    merge06_02= merge06_01.drop(['id_nivel','activo', 'id'], axis=1)
    
    merge07_01 = merge07.rename(columns={'nombre': 'razon'})
    merge07_02= merge07_01.drop(['id_razon','activo', 'id'], axis=1)
    
    merge09_01 = merge09.rename(columns={'nombre': 'tipo_pobreza'})
    merge09_02= merge09_01.drop(['id_tipo','activo', 'id'], axis=1)
    
    merge10_01 = merge10.rename(columns={'nombre': 'tipo_tenencia'})
    merge10_02= merge10_01.drop(['id_tipo_tenecia','activo', 'id'], axis=1)
    
    merge11_01 = merge11.rename(columns={'nombre_x': 'municipio'})
    merge11_02 = merge11_01.rename(columns={'nombre_y': 'material'})
    merge11_03 = merge11_02.drop(['clave','id', 'activo_x', 'activo_y','id_material'], axis=1)
    
    
    claves2 = []
    for i in merge05_02.keys():
        claves2.append(i)
    for i in merge06_02.keys():
        claves2.append(i)
    for i in merge07_02.keys():
        claves2.append(i)
    for i in merge09_02.keys():
        claves2.append(i)
    for i in merge10_02.keys():
        claves2.append(i)
    for i in merge11_03.keys():
        claves2.append(i)
    for i in df13.keys():
        claves2.append(i) 
    
    final_list2 = list(OrderedDict.fromkeys(claves2))
    
    
    difference_1 = set(final_list2).difference(set(merge05_02))
    difference_2 = set(merge05_02).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    merge05_02['trimestre']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['nivel']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['viviendas']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['decil']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['material']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['razon']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['id']=pd.Series([],dtype=pd.StringDtype())
    merge05_02['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list2).difference(set(merge06_02))
    difference_2 = set(merge06_02).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge06_02['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['viviendas']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['decil']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['material']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['condicion']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['razon']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['id']=pd.Series([],dtype=pd.StringDtype())
    merge06_02['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    
    difference_1 = set(final_list2).difference(set(merge07_02))
    difference_2 = set(merge07_02).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge07_02['nivel']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['viviendas']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['decil']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['material']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['condicion']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['id']=pd.Series([],dtype=pd.StringDtype())
    merge07_02['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list2).difference(set(merge09_02))
    difference_2 = set(merge09_02).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge09_02['trimestre']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['nivel']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['viviendas']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['decil']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['material']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['condicion']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['razon']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['id']=pd.Series([],dtype=pd.StringDtype())
    merge09_02['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list2).difference(set(merge10_02))
    difference_2 = set(merge10_02).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    merge10_02['trimestre']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['nivel']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['viviendas']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['decil']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['poblacion']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['material']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['condicion']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['razon']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['id']=pd.Series([],dtype=pd.StringDtype())
    merge10_02['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list2).difference(set(merge11_03))
    difference_2 = set(merge11_03).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    merge11_03['trimestre']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['nivel']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['ingreso']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['decil']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['poblacion']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['condicion']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['razon']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['id']=pd.Series([],dtype=pd.StringDtype())
    merge11_03['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list2).difference(set(df13))
    difference_2 = set(df13).difference(set(final_list2))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    df13['trimestre']=pd.Series([],dtype=pd.StringDtype())
    df13['nivel']=pd.Series([],dtype=pd.StringDtype())
    df13['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    df13['viviendas']=pd.Series([],dtype=pd.StringDtype())
    df13['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    df13['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    df13['material']=pd.Series([],dtype=pd.StringDtype())
    df13['condicion']=pd.Series([],dtype=pd.StringDtype())
    df13['razon']=pd.Series([],dtype=pd.StringDtype())
    df13['id_municipio']=pd.Series([],dtype=pd.StringDtype())
    df13['municipio']=pd.Series([],dtype=pd.StringDtype())
    df13['poblacion']=pd.Series([],dtype=pd.StringDtype())
    df13['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    
    
    merge05_new = merge05_02[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    merge06_new = merge06_02[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    merge07_new = merge07_02[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    merge09_new = merge09_02[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    merge10_new = merge10_02[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    merge11_new = merge11_03[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    df13_new = df13[['condicion', 'anio', 'poblacion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'porcentaje', 'id_municipio', 
                              'municipio', 'id_entidad', 'material', 'viviendas', 'id', 'decil',
                              'ingreso']]
    
    
    Ine = pd.concat([merge05_new,merge06_new,merge07_new,
                         merge09_new, merge10_new, merge11_new,
                        df13_new])
    
    Ine['id_entidad'] = Ine['id_entidad'].replace([15], ['MÉXICO']) 
    Ine01 = Ine.rename(columns={'id_entidad': 'entidad'})
    Ine02 = Ine01.drop(['id_municipio'], axis=1)
    Ine03 = Ine02.drop(['id'], axis=1)
    
    
    Ine_final = Ine03
    
    
    
    merge12 = pd.merge(df30, df25_new, on='id_municipio')
    merge12_01 = merge12.drop(['id', 'id_municipio','clave',
                          'id_entidad_y','activo'], axis=1)
    merge12_01['id_entidad_x'] = merge12_01['id_entidad_x'].replace([15], ['MÉXICO'])
    merge12_02 = merge12_01.rename(columns={'id_entidad_x': 'entidad'})
    merge12_03 = merge12_02.rename(columns={'nombre': 'municipio'})
    
    df26_new = df26.rename(columns={'id': 'id_bien_jur'})
    merge12_04  = pd.merge(merge12_03, df26_new, on='id_bien_jur')
    merge12_05 = merge12_04.drop(['activo'], axis=1)
    merge12_06 = merge12_05.rename(columns={'nombre': 'bienes_juridicos'})
    merge12_07 = merge12_06.drop(['id_bien_jur'], axis=1)
    
    df36_new = df36.rename(columns={'id': 'id_tipo_del'})
    merge12_08  = pd.merge(merge12_07, df36_new, on='id_tipo_del')
    merge12_09 = merge12_08.drop(['activo'], axis=1)
    merge12_10 = merge12_09.rename(columns={'nombre': 'tipos_delito'})
    merge12_11 = merge12_10.drop(['id_tipo_del'], axis=1)
    
    df35_new = df35.rename(columns={'id': 'id_subtipo_del'})
    merge12_12  = pd.merge(merge12_11, df35_new, on='id_subtipo_del')
    merge12_13 = merge12_12.drop(['activo'], axis=1)
    merge12_14 = merge12_13.rename(columns={'nombre': 'subtipos_delito'})
    merge12_15 = merge12_14.drop(['id_subtipo_del'], axis=1)
    
    df33_new = df33.rename(columns={'id': 'id_mod_del'})
    merge12_16  = pd.merge(merge12_15, df33_new, on='id_mod_del')
    merge12_17 = merge12_16.drop(['activo'], axis=1)
    merge12_18 = merge12_17.rename(columns={'nombre': 'modalidades_delito'})
    merge12_19 = merge12_18.drop(['id_mod_del'], axis=1)
    
    
    df37_new = df37.rename(columns={'id': 'id_tipo_del'})
    merge13 = pd.merge(df31, df37_new, on='id_tipo_del')
    merge13_01 = merge13.drop(['activo'], axis=1)
    merge13_02 = merge13_01.rename(columns={'nombre': 'tipos_delito_ff'})
    merge13_03 = merge13_02.drop(['id_tipo_del'], axis=1)
    merge13_03['id_entidad'] = merge13_03['id_entidad'].replace([15], ['MÉXICO'])
    merge13_04 = merge13_03.drop(['id'], axis=1)
    merge13_05 = merge13_04.rename(columns={'id_entidad': 'entidad'})
    
    df32_new = df32.rename(columns={'id': 'id_ley'})
    merge13_06 = pd.merge(merge13_05, df32_new, on='id_ley')
    merge13_07 = merge13_06.drop(['activo'], axis=1)
    merge13_08 = merge13_07.rename(columns={'nombre': 'leyes_ff'})
    merge13_09 = merge13_08.drop(['id_ley'], axis=1)
    
    df27_new = df27.rename(columns={'id': 'id_concepto_del'})
    merge13_10 = pd.merge(merge13_09, df27_new, on='id_concepto_del')
    merge13_11 = merge13_10.drop(['activo'], axis=1)
    merge13_12 = merge13_11.rename(columns={'nombre': 'conceptos_delito_ff'})
    merge13_13 = merge13_12.drop(['id_concepto_del'], axis=1)
    
    df29_new = df29.rename(columns={'id': 'id_genero'})
    merge14 = pd.merge(df39, df29_new, on='id_genero')
    
    df34_new = df34.rename(columns={'id': 'id_rango_edad'})
    merge14_01 = pd.merge(merge14, df34_new, on='id_rango_edad')
    merge14_02 = merge14_01.drop(['activo_x', 'activo_y'], axis=1)
    merge14_03 = merge14_02.rename(columns={'nombre': 'generos'})
    merge14_04 = merge14_03.rename(columns={'descripcion': 'rangos_edad'})
    merge14_05 = merge14_04.drop(['id_rango_edad', 'id_genero'], axis=1)
    
    merge14_06 = pd.merge(merge14_05, df26_new, on='id_bien_jur')
    merge14_07 = merge14_06.rename(columns={'nombre': 'bienes_juridicos'})
    merge14_08 = merge14_07.drop(['activo', 'id_bien_jur'], axis=1)
    
    merge14_09 = pd.merge(merge14_08, df36_new, on='id_tipo_del')
    merge14_10 = merge14_09.rename(columns={'nombre': 'tipos_delito'})
    merge14_11 = merge14_10.drop(['activo', 'id_tipo_del'], axis=1)
    
    merge14_12 = pd.merge(merge14_11, df35_new, on='id_subtipo_del')
    merge14_13 = merge14_12.rename(columns={'nombre': 'subtipos_delito'})
    merge14_14 = merge14_13.drop(['activo', 'id_subtipo_del'], axis=1)
    
    merge14_15 = pd.merge(merge14_14, df33_new, on='id_mod_del')
    merge14_16 = merge14_15.rename(columns={'nombre': 'modalidades_delito'})
    merge14_17 = merge14_16.drop(['activo', 'id_mod_del'], axis=1)
    
    merge14_18 = merge14_17.drop(['id'], axis=1)
    merge14_18['id_entidad'] = merge14_18['id_entidad'].replace([15], ['MÉXICO']) 
    merge14_19 = merge14_18.rename(columns={'id_entidad': 'entidad'})
    
    
    
    merge15 = pd.merge(df28, df36_new, on='id_tipo_del')
    merge15_01 = merge15.drop(['id','id_tipo_del' ], axis=1)
    merge15_02 = merge15_01.rename(columns={'nombre': 'delitos_c100_habitantes'})
    
    merge15_03 = pd.merge(merge15_02, df35_new, on='id_subtipo_del')
    merge15_04 = merge15_03.drop(['activo_x', 'activo_y'], axis=1)
    merge15_05 = merge15_04.rename(columns={'nombre': 'subtipos_delito'})
    
    merge15_06 = pd.merge(merge15_05, df11_new, on='id_entidad')
    merge15_07 = merge15_06.drop(['activo', 'clave', 'id_entidad', 'id_subtipo_del'], axis=1)
    merge15_08 = merge15_07.rename(columns={'nombre': 'entidad'})
    
    
    merge16 = pd.merge(df38, df36_new, on='id_tipo_del')
    merge16_01 = merge16.drop(['activo', 'id'], axis=1)
    merge16_02 = merge16_01.rename(columns={'nombre': 'tipos_delito'})
    merge16_03 = merge16_02.drop(['id_tipo_del'], axis=1)
    
    merge16_04 = pd.merge(merge16_03, df35_new, on='id_subtipo_del')
    merge16_05 = merge16_04.drop(['activo', 'id_subtipo_del'], axis=1)
    merge16_06 = merge16_05.rename(columns={'nombre': 'subtipos_delito'})
    
    merge16_07 = pd.merge(merge16_06, df33_new, on='id_mod_del')
    merge16_08 = merge16_07.drop(['activo', 'id_mod_del'], axis=1)
    merge16_09 = merge16_08.rename(columns={'nombre': 'modalidades_delito'})
    
    
    claves3 = []
    for i in merge12_19.keys():
        claves3.append(i)
    for i in merge13_13.keys():
        claves3.append(i)
    for i in merge14_19.keys():
        claves3.append(i)
    for i in merge15_08.keys():
        claves3.append(i)
    for i in merge16_09.keys():
        claves3.append(i)
    
    final_list3 = list(OrderedDict.fromkeys(claves3))
    
    
    difference_1 = set(final_list3).difference(set(merge12_19))
    difference_2 = set(merge12_19).difference(set(final_list3))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    merge12_19['unidades']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['generos']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    merge12_19['capital']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list3).difference(set(merge13_13))
    difference_2 = set(merge13_13).difference(set(final_list3))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    merge13_13['tipos_delito']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['bienes_juridicos']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['unidades']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['subtipos_delito']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['generos']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['modalidades_delito']=pd.Series([],dtype=pd.StringDtype())
    merge13_13['capital']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list3).difference(set(merge14_19))
    difference_2 = set(merge14_19).difference(set(final_list3))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge14_19['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['unidades']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['capital']=pd.Series([],dtype=pd.StringDtype())
    merge14_19['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list3).difference(set(merge15_08))
    difference_2 = set(merge15_08).difference(set(final_list3))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge15_08['tipos_delito']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['bienes_juridicos']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['unidades']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['generos']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['modalidades_delito']=pd.Series([],dtype=pd.StringDtype())
    merge15_08['mes']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list3).difference(set(merge16_09))
    difference_2 = set(merge16_09).difference(set(final_list3))
    
    list_difference = list(difference_1.union(difference_2))
    
    merge16_09['bienes_juridicos']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['generos']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['incidencia']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['entidad']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['municipio']=pd.Series([],dtype=pd.StringDtype())
    merge16_09['capital']=pd.Series([],dtype=pd.StringDtype())
    
    
        
    merge12_19_new = merge12_19[['entidad', 'anio', 'mes', 'incidencia', 'municipio', 
                                 'bienes_juridicos', 'tipos_delito', 'subtipos_delito', 
                                 'modalidades_delito', 'tipos_delito_ff', 'leyes_ff', 
                                 'conceptos_delito_ff', 'generos', 'rangos_edad', 
                                 'delitos_c100_habitantes', 'capital', 'unidades']]
    merge13_13_new = merge13_13[['entidad', 'anio', 'mes', 'incidencia', 'municipio', 
                                 'bienes_juridicos', 'tipos_delito', 'subtipos_delito', 
                                 'modalidades_delito', 'tipos_delito_ff', 'leyes_ff', 
                                 'conceptos_delito_ff', 'generos', 'rangos_edad', 
                                 'delitos_c100_habitantes', 'capital', 'unidades']]
    merge14_19_new = merge14_19[['entidad', 'anio', 'mes', 'incidencia', 'municipio', 
                                 'bienes_juridicos', 'tipos_delito', 'subtipos_delito', 
                                 'modalidades_delito', 'tipos_delito_ff', 'leyes_ff', 
                                 'conceptos_delito_ff', 'generos', 'rangos_edad', 
                                 'delitos_c100_habitantes', 'capital', 'unidades']]
    merge15_08_new = merge15_08[['entidad', 'anio', 'mes', 'incidencia', 'municipio', 
                                 'bienes_juridicos', 'tipos_delito', 'subtipos_delito', 
                                 'modalidades_delito', 'tipos_delito_ff', 'leyes_ff', 
                                 'conceptos_delito_ff', 'generos', 'rangos_edad', 
                                 'delitos_c100_habitantes', 'capital', 'unidades']]
    merge16_09_new = merge16_09[['entidad', 'anio', 'mes', 'incidencia', 'municipio', 
                                 'bienes_juridicos', 'tipos_delito', 'subtipos_delito', 
                                 'modalidades_delito', 'tipos_delito_ff', 'leyes_ff', 
                                 'conceptos_delito_ff', 'generos', 'rangos_edad', 
                                 'delitos_c100_habitantes', 'capital', 'unidades']]
    
    
    Seg = pd.concat([merge12_19_new,merge13_13_new,merge14_19_new,
                 merge15_08_new, merge16_09_new])
    
    claves4 = []
    for i in Coneval_final.keys():
        claves4.append(i)
    for i in Ine_final.keys():
        claves4.append(i)
    for i in Seg.keys():
        claves4.append(i)
    
    final_list4 = list(OrderedDict.fromkeys(claves4))
    
    
    difference_1 = set(final_list4).difference(set(Coneval_final))
    difference_2 = set(Coneval_final).difference(set(final_list4))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    Coneval_final['viviendas']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['tipos_delito']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['condicion']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['bienes_juridicos']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['generos']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['incidencia']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['municipio']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['material']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['modalidades_delito']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['mes']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['decil']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['razon']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['unidades']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['subtipos_delito']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['trimestre']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['ingreso']=pd.Series([],dtype=pd.StringDtype())
    Coneval_final['nivel']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list4).difference(set(Ine_final))
    difference_2 = set(Ine_final).difference(set(final_list4))
    
    list_difference = list(difference_1.union(difference_2))
    
    Ine_final['tipos_delito']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['bienes_juridicos']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['conceptos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['incidencia']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['generos']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['rangos_edad']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['modalidades_delito']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['capital']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['mes']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['profundidad']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['unidades']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['intensidad']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['subtipos_delito']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['personas']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['tipos_delito_ff']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['indicador_pobreza']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['leyes_ff']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['promedio']=pd.Series([],dtype=pd.StringDtype())
    Ine_final['delitos_c100_habitantes']=pd.Series([],dtype=pd.StringDtype())
    
    
    difference_1 = set(final_list4).difference(set(Seg))
    difference_2 = set(Seg).difference(set(final_list4))
    
    list_difference = list(difference_1.union(difference_2))
    
    
    Seg['viviendas']=pd.Series([],dtype=pd.StringDtype())
    Seg['condicion']=pd.Series([],dtype=pd.StringDtype())
    Seg['porcentaje']=pd.Series([],dtype=pd.StringDtype())
    Seg['id_entidad']=pd.Series([],dtype=pd.StringDtype())
    Seg['material']=pd.Series([],dtype=pd.StringDtype())
    Seg['tipo_tenencia']=pd.Series([],dtype=pd.StringDtype())
    Seg['decil']=pd.Series([],dtype=pd.StringDtype())
    Seg['razon']=pd.Series([],dtype=pd.StringDtype())
    Seg['profundidad']=pd.Series([],dtype=pd.StringDtype())
    Seg['intensidad']=pd.Series([],dtype=pd.StringDtype())
    Seg['personas']=pd.Series([],dtype=pd.StringDtype())
    Seg['tipo_pobreza']=pd.Series([],dtype=pd.StringDtype())
    Seg['indicador_pobreza']=pd.Series([],dtype=pd.StringDtype())
    Seg['promedio']=pd.Series([],dtype=pd.StringDtype())
    Seg['trimestre']=pd.Series([],dtype=pd.StringDtype())
    Seg['poblacion']=pd.Series([],dtype=pd.StringDtype())
    Seg['ingreso']=pd.Series([],dtype=pd.StringDtype())
    Seg['nivel']=pd.Series([],dtype=pd.StringDtype())
    
    
    
    Coneval_final_new = Coneval_final[['indicador_pobreza', 'id_entidad', 'anio', 'porcentaje', 'personas', 
                              'promedio', 'entidad', 'capital', 'poblacion', 'profundidad', 
                              'intensidad', 'condicion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'municipio', 'material', 'viviendas', 
                              'decil', 'ingreso', 'mes', 'incidencia', 'bienes_juridicos', 
                              'tipos_delito', 'subtipos_delito', 'modalidades_delito', 
                              'tipos_delito_ff', 'leyes_ff', 'conceptos_delito_ff', 'generos',
                              'rangos_edad', 'delitos_c100_habitantes', 'unidades']]
    
    Ine_final_new = Ine_final[['indicador_pobreza', 'id_entidad', 'anio', 'porcentaje', 'personas', 
                              'promedio', 'entidad', 'capital', 'poblacion', 'profundidad', 
                              'intensidad', 'condicion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'municipio', 'material', 'viviendas', 
                              'decil', 'ingreso', 'mes', 'incidencia', 'bienes_juridicos', 
                              'tipos_delito', 'subtipos_delito', 'modalidades_delito', 
                              'tipos_delito_ff', 'leyes_ff', 'conceptos_delito_ff', 'generos',
                              'rangos_edad', 'delitos_c100_habitantes', 'unidades']]
    
    Seg_new = Seg[['indicador_pobreza', 'id_entidad', 'anio', 'porcentaje', 'personas', 
                              'promedio', 'entidad', 'capital', 'poblacion', 'profundidad', 
                              'intensidad', 'condicion', 'nivel', 'trimestre', 'razon', 
                              'tipo_pobreza', 'tipo_tenencia', 'municipio', 'material', 'viviendas', 
                              'decil', 'ingreso', 'mes', 'incidencia', 'bienes_juridicos', 
                              'tipos_delito', 'subtipos_delito', 'modalidades_delito', 
                              'tipos_delito_ff', 'leyes_ff', 'conceptos_delito_ff', 'generos',
                              'rangos_edad', 'delitos_c100_habitantes', 'unidades']]
    
    
    Cygni = pd.concat([Coneval_final_new,
                    Ine_final_new, Seg_new ])

    return Cygni


if __name__ == "__main__":
    host = "localhost"
    user =  "cristhiamdaniel"
    passwd = "daniel"
    database = "cygni_edomex"
    
    df = procesamientoDF(host, user, passwd, database)
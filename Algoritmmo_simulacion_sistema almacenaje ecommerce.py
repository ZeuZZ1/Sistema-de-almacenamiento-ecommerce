# -*- coding: utf-8 -*-
"""
Created on Thu Apr 20 12:26:54 2023

Simulación sistema de almacenaje para la bodega ecommerce Tricot.
Este programa se encarga de apoyar en la decisión de almacenamiento de la merca
dería en la bodega de ecommerce de Tricot. Para completar la tarea, se valdrá
de información actual del stock en la bodega, información actual de las ubica
ciones de almacenamiento y de la venta/rotación de unidades de los productos 
sobre una base departamento-línea, sacando la información desde
WMS y ERP Tricot, respectivamente.

Como resultado, entregará un template de asignación y almacenamiento para WMS y la instrucción
de destino para WCS para el transporte de cajas 

Los archivos que se usarán como referencia aquí serán solo ejemplos representati
vos de la información original.

Características principales que considera el sistema y desarrollo:
    -Se usará 3 zonas de trabajo
    -Se usará el piso 1 para rebalse
    -Se usará el formato de depto-línea para categoría de venta
    -Se usará formato de info entregado por ecommerce para representar la venta depto-línea
    -Se usará template de WMS para entregar las ubicaciones para la asignación de almacenamiento
    -Se usará una lista que relacione LPN-Destino WCS para entregar info a WCS con el mensaje SendtransportOrder_WCS
    -Se usará modelo de reportería: Conf_ubicaciones y inv_activo desde WMS

@author: bpineda
"""

import pandas as pd
import numpy as np
import os 
import math

'''Funciones'''

def mant_sinasignar():
    ''' Función que se encarga de darle el formato correcto a los valores de la tabla
    del  mantenedor sin asignar. Formato correcto, int'''
    for columna in mantenedor_sinasignar.columns:
        mantenedor_sinasignar[columna]=pd.to_numeric(mantenedor_sinasignar[columna]) #Para que funcione el pd.idxmax
        
def mant_asignar():    
    ''' Función que se encarga de darle el formato correcto a los valores de la tabla
    del  mantenedor asignar. Formato correcto, int'''
    for columna in mantenedor_asignar.columns:
        mantenedor_asignar[columna]=pd.to_numeric(mantenedor_asignar[columna]) #Para que funcione el pd.idxmax
        
def mant_almacenamiento():    
    ''' Función que se encarga de darle el formato correcto a los valores de la tabla
    del  mantenedor almacenamiento. Formato correcto, int'''
    for columna in mantenedor_almacenamiento.columns:
        mantenedor_almacenamiento[columna]=0 #Para que funcione el pd.idxmax

def separa_codbarra(i):
    '''Función que se encarga de separar los valores provenientes de las tablas descargadas desde WMS'''
    try:
        return str(i.split('"')[1])
    except:
        return 0
    
def categoria(i, percentil50, percentil80):
    '''Función que se encarga de establecer los percentiles que definirán los tramos de rotación por deptolínea
    A: categoría de venta mayor. solo los productos que tienen una rotación por sobre o igual al 80% más vendido.
    B: categoría de venta intermedia. solo los productos que tienen una rotación menor al 80% más vendido y mayor o igual 50% más vendido.
    C: categoría de venta menor. solo los productos que tienen una rotación menor al 50% más vendido y mayor a 0.
    D: categoría de venta dummy que se encarga de agrupar todos los depto línea que no tienen venta, o sea, venta = 0.'''
    a=percentil50
    b=percentil80
    if i>=b:
        return 'A'
    elif i<b and i>=a:
        return 'B'
    elif i<a and i>0: 
        return 'C'
    else:
        return 'D'
    
def asignacion_disponible(i):
    '''Función para dar valor al campo Asignación
    1: para ubicación asignada
    0: para ubicación no asignada con SKU'''    
    if bd_ubicaciones_contenido.at[i.name,'Asignacion'] is np.nan:
        return 0
    else:
        return 1
    
def consulta_espacios_asignar(i):
    '''Consulta de espacios disponibles asignados'''
    for j in categorias:
        items_cubiculo=int(bd_ubicaciones_contenido[(bd_ubicaciones_contenido['Asignacion']==1)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==i.name)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==j)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])][dic_campos_asigna['EspacioCubiculo']].sum()) 
        mantenedor_asignar.at[i.name,j]=items_cubiculo
        
def consulta_espacios_sin_asignar(i):
    '''Consulta de espacios disponibles no asignados'''
    for j in categorias:
        items_cubiculo=int(bd_ubicaciones_contenido[(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==i.name)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==j)][dic_campos_asigna['EspacioCubiculo']].sum())
        mantenedor_sinasignar.at[i.name,j]=items_cubiculo
        
def consulta_espacio_total():
    '''Módulo para verificar que queda algo de esapcio libre, ya sea asignado o no'''
    if max(mantenedor_sinasignar.max())==0 and max(mantenedor_asignar.max())==0:
    # No queda espacio y es necesario que todas las demás cajas del ASN sean destinadas al piso 1
        return 1
    else: return 0
    # Consultarse esto cada vez que se quiera almacenar una caja
def destinar_lpn_piso1():
    '''Función que se encarga de destinor algún LPN al piso 1 de la mezzanine cuando no encuentra espacio para ser almacenado.
    Entrega el valor de destino a WCS correspondiente a salida del nivel 1.
    No hace nada más'''
    global lpn
    global columnas_lpndestino
    global wcs_lpndestino
    append_lpndestino=(lpn[0],dic_salidas_wcs['EC01'])#datos para el wcs template 
    append_lpndestino=pd.DataFrame(data=append_lpndestino,index=columnas_lpndestino).T#datos para el wms template 
    wcs_lpndestino=pd.concat([wcs_lpndestino,append_lpndestino])
    wcs_lpndestino.drop_duplicates(keep='first',inplace=True)
    return 0

def lpn_no_almacenado():
    '''Funcion que se encarga de actualizar la lista de LPN que no pudieron ser almacenados y fueron destinados al piso 1 de la mezzanine'''
    global lista_no_almacen
    global campos_lista_no_almacen
    append_lpnnoalmacen=(lpn[0],lpn[1],lpn[2],lpn[3])#datos para la lista de no almacenaje
    append_lpnnoalmacen=pd.DataFrame(data=append_lpnnoalmacen,index=campos_lista_no_almacen).T#datos para el wms template 
    lista_no_almacen=pd.concat([lista_no_almacen,append_lpnnoalmacen])
    lista_no_almacen.drop_duplicates(keep='first',inplace=True)
    return 0
    
def actualizacion_templates(i,unidad_almacen):
    '''Función para actualizar los templates que se van generado en la operación de almacenamiento.
    WMS template: template para asignar ubicaciones con SKU
    WCS LPN-destino: template para enviar cajas a las distintas salidas de WMS
    lista almacenamiento: lista con los detalles de donde debiese estar almacenado un LPN dado
    lista no almacenamiento: lista de LPN que no pudieron ser almacenados
    mantenedor sin asignar: mantenedor de ubicaciones que muestran espacios disponibles sin asignar por zona-categoría
    mantenedor de almacenamiento: mantenedor de ubicaciones que muestran las unidades almancenadas por zona-categoría'''
    global lista_almacen
    global wms_template
    global wcs_lpndestino
    global dic_campos_ubicaciones
    # global unidad_almacen
    # print('templates')
    area=ubicaciones.at[i,dic_campos_ubicaciones['Mascara']].split('-')[0]
    aisle=ubicaciones.at[i,dic_campos_ubicaciones['Mascara']].split('-')[1]
    bay=ubicaciones.at[i,dic_campos_ubicaciones['Mascara']].split('-')[2]
    level=ubicaciones.at[i,dic_campos_ubicaciones['Mascara']].split('-')[3]
    pick_sequence='Buscar'
    barcode=ubicaciones.at[i,dic_campos_ubicaciones['CodigoUbic']]
    putaway_seq='Buscar'
    item_assignment='P'
    item_alternate_code=lpn[1]
    task_zone='Buscar'
    cust_field='Buscar'
    append_lista_almacen=(lpn[0],lpn[1],zona_almacenar,ubicaciones.at[i,dic_campos_ubicaciones['Mascara']],ubicaciones.at[i,dic_campos_ubicaciones['CodigoUbic']],unidad_almacen,lpn[2])#datos para la lista de almacenamiento 
    append_wms_template=(area,aisle,bay,level,pick_sequence,barcode,putaway_seq,item_assignment,item_alternate_code,task_zone,cust_field)#datos para el wms template 
    append_lpndestino=(lpn[0],dic_salidas_wcs[ubicaciones.at[i,dic_campos_ubicaciones['Mascara']][0:4]])#datos para el wcs template 
    append_lista_almacen=pd.DataFrame(data=append_lista_almacen,index=campos_lista_almacenamiento).T
    append_wms_template=pd.DataFrame(data=append_wms_template,index=dic_campos_wms_template_asignacion).T
    append_lpndestino=pd.DataFrame(data=append_lpndestino,index=columnas_lpndestino).T#datos para el wms template 
    lista_almacen=pd.concat([lista_almacen,append_lista_almacen])
    wms_template=pd.concat([wms_template,append_wms_template])
    wcs_lpndestino=pd.concat([wcs_lpndestino,append_lpndestino])
    wms_template.drop_duplicates(keep='first',inplace=True)
    wcs_lpndestino.drop_duplicates(keep='first',inplace=True)
    
def actualizacion_sinasignar(i,unidad_almacen):
    '''Actualizacion de mantenedor de espacios disponibles sin asignar'''
    global bd_ubicaciones_contenido
    global dic_campos_asigna
    # print('sinasignar')
    bd_ubicaciones_contenido.at[i,dic_campos_asigna['ItemsCubiculo']]=bd_ubicaciones_contenido.at[i,dic_campos_asigna['ItemsCubiculo']]+unidad_almacen
    bd_ubicaciones_contenido.at[i,dic_campos_asigna['EspacioCubiculo']]=bd_ubicaciones_contenido.at[i,dic_campos_asigna['EspacioCubiculo']]-unidad_almacen
    bd_ubicaciones_contenido.at[i,'Asignacion']=1
    bd_ubicaciones_contenido.at[i,dic_campos_asigna['CodigoSKU']]=lpn[1]
    
def actualizacion_asignar(i,unidad_almacen):
    '''Actualizacion de mantenedor de espacios disponibles asignadas'''
    global lpn
    global bd_ubicaciones_contenido
    # print('asignar')
    bd_ubicaciones_contenido.at[i,dic_campos_asigna['ItemsCubiculo']]=bd_ubicaciones_contenido.at[i,dic_campos_asigna['ItemsCubiculo']]+unidad_almacen
    bd_ubicaciones_contenido.at[i,dic_campos_asigna['EspacioCubiculo']]=bd_ubicaciones_contenido.at[i,dic_campos_asigna['EspacioCubiculo']]-unidad_almacen

def almacenar_ubicacion(i):
    '''Función para almacenar un LPN en las ubicaciones escogidas por el algoritmo'''
    global lpn
    global capacidad_cubiculo
    global categoria_i
    global ubicaciones
    global zona_almacenar
    global dic_campos_ubicaciones
    global dic_campos_asigna
    global mant_sinasignar
    global mant_asignar
    global proceso
    global bd_ubicaciones_contenido
    # print('almacenar_ubicacion')
    # print(categoria_i)
    # print(lpn,lpn_asignado,zona_almacenar,categoria_i)
    # print(ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']])
    # print(proceso)
    if lpn[2]==0:
        # print('1:','LPN',lpn,'sin unidades para almacenar')
        # break
        return 0
    elif lpn[2]!=0:
        # print('2',ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']])
        if ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]==0:#consultar por si la ubicacion tiene espacio
        # if ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]==0:#prueba
            # print('2.1: Cubiculo sin espacio')
            return 0
            # data_wms_template=(lpn[0],dic_salidas_wcs[ubicaciones.at[i.name,dic_campos_ubicaciones['Mascara']][0:4]])#datos para el LPN DESTINO 
        elif ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]==lpn[2]:
        # elif ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]==lpn[2]:#prueba
            # print('2.2: Cubiculo con igual espacio que el LPN')
            # unidades_almacenadas=lpn[2]
            unidad_almacen=lpn[2]
            # espacio_remanente=ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]-lpn[2]#=0
            if proceso=='Asignado':
                mantenedor_asignar.at[zona_almacenar,categoria_i]=mantenedor_asignar.at[zona_almacenar,categoria_i]-lpn[2]
                # print('10.1',mantenedor_asignar.at[zona_almacenar,categoria_i])
            elif proceso=='No asignado':
                mantenedor_sinasignar.at[zona_almacenar,categoria_i]=mantenedor_sinasignar.at[zona_almacenar,categoria_i]-capacidad_cubiculo
                # print('10.2',mantenedor_sinasignar.at[zona_almacenar,categoria_i])
            mantenedor_almacenamiento.at[zona_almacenar,categoria_i]=mantenedor_almacenamiento.at[zona_almacenar,categoria_i]+unidad_almacen
            ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]=ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]-lpn[2]
            # lpn[2]=lpn[2]-ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]#=0
            lpn[2]=0#=0
            actualizacion_templates(i.name,unidad_almacen)
            if proceso=='No asignado':
                actualizacion_sinasignar(i.name,unidad_almacen)
            elif proceso=='Asignado':
                actualizacion_asignar(i.name,unidad_almacen)
                
                
        elif ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]>lpn[2]:
        # elif ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]>lpn[2]:
            # print('2.3: Cubiculo con más espacio que el LPN')
            unidad_almacen=lpn[2]
            # espacio_remanente=ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]-lpn[2]
            # espacio_remanente=ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]-lpn[2]#prueba
            ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]=ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]-lpn[2]
            # ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]=ubicaciones.at[551,dic_campos_asigna['EspacioCubiculo']]-lpn[2]#prueba
            if proceso=='Asignado':
                mantenedor_asignar.at[zona_almacenar,categoria_i]=mantenedor_asignar.at[zona_almacenar,categoria_i]-lpn[2]
                # print('10.3',mantenedor_asignar.at[zona_almacenar,categoria_i])
            elif proceso=='No asignado':
                mantenedor_sinasignar.at[zona_almacenar,categoria_i]=mantenedor_sinasignar.at[zona_almacenar,categoria_i]-capacidad_cubiculo
                # print('10.4',mantenedor_sinasignar.at[zona_almacenar,categoria_i])
            mantenedor_almacenamiento.at[zona_almacenar,categoria_i]=mantenedor_almacenamiento.at[zona_almacenar,categoria_i]+unidad_almacen
            lpn[2]=0
            actualizacion_templates(i.name,unidad_almacen)
            if proceso=='No asignado':
                actualizacion_sinasignar(i.name,unidad_almacen)
            elif proceso=='Asignado':
                actualizacion_asignar(i.name,unidad_almacen)
            
        elif ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]<lpn[2]:
            # print('2.4: Cubiculo con menos espacio que el LPN')
            unidad_almacen=ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]
            # espacio_remanente=0
            if proceso=='Asignado':
                mantenedor_asignar.at[zona_almacenar,categoria_i]=mantenedor_asignar.at[zona_almacenar,categoria_i]-lpn[2]
                # print('10.5',mantenedor_asignar.at[zona_almacenar,categoria_i])
            elif proceso=='No asignado':
                mantenedor_sinasignar.at[zona_almacenar,categoria_i]=mantenedor_sinasignar.at[zona_almacenar,categoria_i]-capacidad_cubiculo
                # print('10.6',mantenedor_sinasignar.at[zona_almacenar,categoria_i])
            mantenedor_almacenamiento.at[zona_almacenar,categoria_i]=mantenedor_almacenamiento.at[zona_almacenar,categoria_i]+unidad_almacen
            lpn[2]=lpn[2]-ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]
            ubicaciones.at[i.name,dic_campos_asigna['EspacioCubiculo']]=0
            actualizacion_templates(i.name,unidad_almacen)
            if proceso=='No asignado':
                actualizacion_sinasignar(i.name,unidad_almacen)
            elif proceso=='Asignado':
                actualizacion_asignar(i.name,unidad_almacen)
            # pass
'''Diccionarios y listas'''
# =============================================================================
# Diccionario y listas del algoritmo que parametrizan nombres de campos y valores
# de variables usadas en el algoritmo.
# =============================================================================
dic_campos_bd_venta={'DeptoLinea':'LINEA','Rotacion':'UNIDADES','CategoriaActual':'CATEGORIA','CategoriaHistorica':'CATEGORIA2'}
dic_campos_asn={'LPN':'Nro LPN','CodigoSKU':'Codigo','ItemsCaja':'Un Env','Producto':'Producto'}
dic_campos_items={'CodigoSKU':'Cod Barra','Departamento':'itemhierarchy2','Linea':'itemhierarchy3','Producto':'Producto','DeptoLinea':'DeptoLinea'}
dic_campos_ubicaciones={'CodigoUbic':'Cod Barra','CodigoSKU':'Producto','Mascara':'Mascara','Categoria':'Campo Person 1','Zona':'Zona tarea'}
dic_campos_asigna={'CodigoSKU':'Producto','CodigoUbic':'Cod Barra Ubic','ItemsCubiculo':'UnAct','EspacioCubiculo':'UnAct_Disponible'}
dic_campos_wms_template_asignacion=['area','aisle','bay','level','pick_seq','barcode','putaway_seq','item_assignment_type_code','item_alternate_code','task_zone_code','cust_field_1']
dic_salidas_wcs={'EC01':'403003','EC02':'405002','EC03':'405004'}
columnas_lpndestino=['LPN','DESTINO']
categorias=['A','B','C','D']
capacidad_cubiculo=30
campos_lista_almacenamiento=('LPN','CodigoSKU','ZonaAlmacenamiento','UbicacionAlmacenamiento','CodigoBarraUbicaion','UnidadesAlmacenadas','UnidadesPorAlmacenar')
campos_lista_no_almacen=('LPN','SKU','Items','CategoriaActual')
dic_secuencia_asignar={'BA':('BAC'),'AB':('ABC'),'CA':('CAB'),'AC':('ACB'),'BC':('BCA'),'CB':('CBA'),'AA':('ABC'),'BB':('BCA'),'CC':('CBA'),'DD':('CBA'),'AD':('ABC'),'BD':('BCA'),'CD':('CBA'),'DA':('CBA'),'DB':('CBA'),'DC':('CBA')}
lista_sinventa=('AD','BD','CD','DA','DB','DC','DD')
dic_secuencia_sinventa={'D':('CBA')}

'''Módulo para identificar la mercadería entrante según WMS'''
# =============================================================================
# En este módulo se identifica cada caja/LPN que entra a la bodega ecommerce
# Se toma como mercadería entrante uno o más ASN que estén registrados en WMS
# Los LPN solo pueden traer una sola categoria DEPTO-LINEA
# Por defecto el LPN toma la categoria que tiene al momento de ser recibida según
# la info de venta/rotación
# Todo LPN es Mono SKU
# =============================================================================

# Estas líneas son provisionales, no son definitivas
bd_asn[dic_campos_asn['CodigoSKU']]=bd_asn[dic_campos_asn['CodigoSKU']].apply(lambda x: str(x))
bd_asn[dic_campos_asn['LPN']]=bd_asn[dic_campos_asn['LPN']].apply(lambda x: str(x))
bd_items[dic_campos_items['CodigoSKU']]=bd_items[dic_campos_items['CodigoSKU']].apply(lambda x: str(x))
bd_asigna['Producto']=bd_asigna['Producto'].apply(lambda x: str(x))


# ASN
bd_asn=pd.read_csv(r'C:\Users\bpineda\Desktop\Proyectos y tareas\PL2510022_Pocket sorter KNAPP\Documentos_Diseño operaciones\Diseño_sistema de almacenaje\archivos de entrada\IBShipmentDtl.csv',sep=';')
bd_asn[dic_campos_asn['LPN']]=bd_asn[dic_campos_asn['LPN']].apply(separa_codbarra)
bd_asn[dic_campos_asn['CodigoSKU']]=bd_asn[dic_campos_asn['CodigoSKU']].apply(separa_codbarra)
bd_asn[dic_campos_asn['Producto']]=bd_asn[dic_campos_asn['CodigoSKU']].apply(lambda x: x[0:6])
bd_lpn=bd_asn[[dic_campos_asn['LPN'], dic_campos_asn['Producto'],dic_campos_asn['CodigoSKU'],dic_campos_asn['ItemsCaja']]]

# maestro productos 
bd_items=pd.read_csv(r'C:\Users\bpineda\Desktop\Proyectos y tareas\PL2510022_Pocket sorter KNAPP\Documentos_Diseño operaciones\Diseño_sistema de almacenaje\archivos de entrada\ItemTRICOT.csv',sep=';')
bd_items[dic_campos_items['CodigoSKU']]=bd_items[dic_campos_items['CodigoSKU']].apply(separa_codbarra)
bd_items=bd_items[bd_items[dic_campos_items['CodigoSKU']]!=0]
bd_items[dic_campos_items['Producto']]=bd_items[dic_campos_items['CodigoSKU']].apply(lambda x: x[0:6])
bd_items[dic_campos_items['DeptoLinea']]=bd_items[dic_campos_items['Departamento']]+bd_items[dic_campos_items['Linea']]
bd_items_deplin=bd_items[[dic_campos_items['Producto'],dic_campos_items['DeptoLinea']]].copy()
bd_items_deplin.drop_duplicates(keep='first',inplace=True)
bd_items_deplin.reset_index(drop=True,inplace=True)

# relacionar ASN con el maestro de productos para relacionar los productos con su DEPTO-LINEA
bd_lpn=pd.merge(bd_lpn,bd_items_deplin, how='left', on=dic_campos_asn['Producto'])[[dic_campos_asn['LPN'],dic_campos_asn['Producto'],dic_campos_asn['CodigoSKU'],dic_campos_asn['ItemsCaja'],dic_campos_items['DeptoLinea']]]

'''Módulo para la obtención de datos de venta/rotación por depto-línea'''
# =============================================================================
# en este módulo se desarrollarán los parámetros de venta que necesita el
# el sistema de almacenaje
# =============================================================================
# BD utilizada para la venta va de Enero a Diciembre 2022

bd_venta_depto_linea= pd.read_excel(r'C:\Users\bpineda\Desktop\Proyectos y tareas\PL2510022_Pocket sorter KNAPP\Documentos_Diseño operaciones\Diseño_sistema de almacenaje\archivos de entrada\bd_venta_depto_linea.xlsx')
lista_rotacion=list(bd_venta_depto_linea[dic_campos_bd_venta['Rotacion']])
percentil50=math.floor(np.percentile(lista_rotacion,50))
percentil80=math.floor(np.percentile(lista_rotacion,80))

# =============================================================================
# Las categorías de venta son 3: A, B y C. Cada una se define por agrupar valores
# que estén dentro de un intervalo de valores definido por ciertos percentiles.
# La categoría A se define como todos los valores que sean mayor o igual al percentil 80
# La categoría B se define como todos los valores que sean mayor o igual al percentil 50 y menores al percentil 80
# La categoría C se define como todos los valores que sean menor que el percentil 50
# Existe la categoría D que agrupa todo depto-lines que no tenga venta registrada
# =============================================================================

bd_venta_depto_linea[dic_campos_bd_venta['CategoriaActual']]=bd_venta_depto_linea[dic_campos_bd_venta['Rotacion']].apply(lambda x: categoria(x,percentil50, percentil80))
bd_venta_depto_linea[dic_campos_bd_venta['CategoriaHistorica']]='D'#Para prueba con la categoria historica
bd_categoria_depto_linea=bd_venta_depto_linea[[dic_campos_bd_venta['DeptoLinea'],dic_campos_bd_venta['CategoriaActual'],dic_campos_bd_venta['CategoriaHistorica']]].copy()

# Relacion de lpn asn con categoría de venta

bd_lpn_deplin=pd.merge(bd_lpn,bd_categoria_depto_linea, how='left', left_on=dic_campos_items['DeptoLinea'],right_on=dic_campos_bd_venta['DeptoLinea'])[[dic_campos_asn['LPN'],dic_campos_asn['CodigoSKU'],dic_campos_asn['ItemsCaja'],dic_campos_bd_venta['CategoriaActual'],dic_campos_bd_venta['CategoriaHistorica']]]
bd_lpn_deplin.drop_duplicates(keep='first')
bd_lpn_deplin=bd_lpn_deplin_aux
bd_lpn_deplin_aux=bd_lpn_deplin.copy()
# bd_lpn_deplin=bd_lpn_deplin[bd_lpn_deplin['Nro LPN'].isin(['2230029524','2230029522','2230029520','2230029518'])]

'''Módulo para la obtención de datos de asignación de ubicaciones según WMS'''
# =============================================================================
# En este módulo se obtendrán las asignaciones de SKU por ubicación.
# Las ubicaciones pueden estar asignadas o no
# Las ubicaciones solo pueden tener un SKU asignado
# Las ubicaciones pueden estar asignadas y con 0 unidades pero sigue siendo una ubicación con asignación
# =============================================================================

# BD asignación de ubicaciones
bd_asigna=pd.read_csv(r'C:\Users\bpineda\Desktop\Proyectos y tareas\PL2510022_Pocket sorter KNAPP\Documentos_Diseño operaciones\Diseño_sistema de almacenaje\archivos de entrada\ActiveInventoryTRICOT.csv',sep=';')
bd_asigna=bd_asigna[[dic_campos_asigna['CodigoUbic'],dic_campos_asigna['CodigoSKU'],dic_campos_asigna['ItemsCubiculo']]]
bd_asigna[dic_campos_asigna['CodigoSKU']]=bd_asigna[dic_campos_asigna['CodigoSKU']].apply(separa_codbarra)

'''Módulo para la obtención de datos de las ubicaciones de activo de WMS'''
# =============================================================================
# En este módulo se obtienen las ubicaciones de activo de WMS
# Las ubicaciones tienen un campo con una letra (A,B o C) que representa la categoría de venta
# =============================================================================

# BD ubicaciones de activo ecommerce
bd_ubicaciones=pd.read_csv(r'C:\Users\bpineda\Desktop\Proyectos y tareas\PL2510022_Pocket sorter KNAPP\Documentos_Diseño operaciones\Diseño_sistema de almacenaje\archivos de entrada\locationTRICOT.csv',sep=';')
bd_ubicaciones=bd_ubicaciones[[dic_campos_ubicaciones['CodigoUbic'],dic_campos_ubicaciones['Mascara'],dic_campos_ubicaciones['Categoria'],dic_campos_ubicaciones['Zona']]]

# Se hace el cruce entre las ubicaciones totales y las asignadas para obtener las disponibles
bd_ubicaciones_contenido=pd.merge(bd_ubicaciones,bd_asigna,how='left',left_on=dic_campos_ubicaciones['CodigoUbic'],right_on=dic_campos_asigna['CodigoUbic'])
bd_ubicaciones_contenido.rename({dic_campos_asigna['CodigoUbic']:'Asignacion'},axis=1,inplace=True)
bd_ubicaciones_contenido['Asignacion']=bd_ubicaciones_contenido.apply(asignacion_disponible,axis=1)
bd_ubicaciones_contenido.sort_values(by=dic_campos_ubicaciones['Mascara'],ascending=True,inplace=True)
bd_ubicaciones_contenido['UnAct']=bd_ubicaciones_contenido['UnAct'].apply(lambda x: x if (x>=0) else 0)
bd_ubicaciones_contenido['UnAct_Disponible']=bd_ubicaciones_contenido['UnAct'].apply(lambda x: capacidad_cubiculo-x if (x<=capacidad_cubiculo) else 0)

# bd_ubicaciones_contenido_aux=bd_ubicaciones_contenido.copy()
# bd_ubicaciones_contenido=bd_ubicaciones_contenido[0:301]
# bd_ubicaciones_contenido=bd_ubicaciones_contenido_aux.copy()

'''Módulo para el mantenedor espacios para sin asignar por zona y categoría'''
# =============================================================================
# Consulta para todo LPN que deba ser almacenado en los espacios sin asignar del almacén
# =============================================================================

mantenedor_sinasignar=pd.DataFrame(index=list(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']].unique()),columns=categorias)
mantenedor_sinasignar.apply(consulta_espacios_sin_asignar,axis=1)

mant_sinasignar()

'''Módulo para el mantenedor unidades almacenadass por zona y categoría'''

mantenedor_almacenamiento=pd.DataFrame(index=list(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']].unique()),columns=categorias)
mant_almacenamiento()

'''Módulo para buscar almacenamiento para un LPN que tiene SKU asignado'''
# =============================================================================
# Consulta para todo LPN que deba ser almacenado en los espacios asignados según su SKU del almacén
# este mantenedor se define cada vez que una caja debe ser almacenada.
# =============================================================================

mantenedor_asignar=pd.DataFrame(index=list(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']].unique()),columns=categorias)

'''Módulo para buscar almacenamiento para un LPN que necesita asignar una ubicación'''
# =============================================================================
# Por LPN se debe realizar esta operación
# Primero se busca almacenar en la categoría correspondiente, si no
# se procura distrinuir dentro de una y solo una zona especifica.
# LPN se compone como: (LPN,CodigoSKU,ItemsCaja,CategoriaActual,CategoriaHistorica)
# =============================================================================

# PEGAR BLOQUE CORRESPONDIENTE
    
'''Módulo para buscar almacenamiento a un LPN en una ubicación ya asignada a su SKU'''
# En este módulo se debe realizar la búsqueda de esapcio para el LPN con un SKU asignado

# PEGAR BLQOUE CORRESPONDIENTE
    
'''Módulo para la lista de almacenamiento, template WMS, info LPN en WCS e info de LPN no almacenados'''
# en este modulo se maneja la información generada a medida que se van almacenando LPN.
lista_almacen=pd.DataFrame(index=[0],columns=campos_lista_almacenamiento)
wms_template=pd.DataFrame(index=[0],columns=dic_campos_wms_template_asignacion)
wcs_lpndestino=pd.DataFrame(index=[0],columns=columnas_lpndestino)
lista_no_almacen=pd.DataFrame(index=[0],columns=campos_lista_no_almacen)

'''Archivos a excel'''
lista_almacen.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\lista_almacen.xls',index=False)
wms_template.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\wms_template.xls',index=False)
wcs_lpndestino.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\wcs_destino.xls',index=False)
lista_no_almacen.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\lista_no_almacen.xls',index=False)
mantenedor_sinasignar.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\mantenedor_sinasignar.xls',index=True)
mantenedor_almacenamiento.to_excel('C:\\Users\\bpineda\\Desktop\\sist almacenaj\\mantenedor_almacenamiento.xls',index=True)

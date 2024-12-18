# -*- coding: utf-8 -*-
"""
Created on Tue May 16 14:57:40 2023

@author: bpineda
"""
# Reemplacé la función almacenar_disponible por todo el codigo al que representa. Cambiar después.


# Para asignacion SKU asignados
mant_sinasignar()

for caja in bd_lpn_deplin.index:
#%%Registro de tiempo inicial
    if caja==bd_lpn_deplin.index[0]:
#%%Datos iniciales de la caja    
        inicio=time.time()
    lpn=list(bd_lpn_deplin.loc[caja])
    lpn_asignado=[1 if bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])&(bd_ubicaciones_contenido['Asignacion']==1)]['Asignacion'].unique() in np.array([1]) else 0]#1 si es que tiene asignacion
    # print(lpn,lpn_asignado)
    lpn_cant_inicial=lpn[2]
#%%Inicializacion del mantenedor asignar con el primer SKU
    mantenedor_asignar=pd.DataFrame(index=list(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']].unique()),columns=categorias)
    mantenedor_asignar.apply(consulta_espacios_asignar,axis=1)
    mant_asignar()
#%%LPN asignado con venta actual sin venta historica
    proceso='Asignado'
    
    if lpn[3]+lpn[4] in lista_sinventa:
        # print('LPN',lpn,'asignado con venta actual y sin venta historica')
        secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
        zona_almacenar=mantenedor_asignar[lpn[3]].idxmax(axis='columns')
        # print(secuencia,zona_almacenar,lpn[0])
        
        if lpn[3]=='D':
            zona_almacenar=mantenedor_asignar['C'].idxmax(axis='columns')
        else:
            zona_almacenar=mantenedor_asignar[lpn[3]].idxmax(axis='columns')
            
        # print('9.2',secuencia,zona_almacenar,lpn[0])
        for categoria_i in secuencia:
            
            if lpn[2]==0:
                # print('9.4 LPN',lpn[0],'almacenado completamente')
                break
            elif mantenedor_asignar.at[zona_almacenar,categoria_i]==0:
                
                # print('9.5 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                if categoria_i==secuencia[-1]:
                    
#%%%Proceso de almacenamiento en ubicaiones disponibles sin asignacion
                    if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                        # Se debe buscar en espacio disponibles sin asignar porque no queda esapcio asignado
                        # print('9.6 LPN',lpn[0],'no almacenado en ubicaciones con asignacion y busca espacio disponible')
                        lpn_asignado=[1 if bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])&(bd_ubicaciones_contenido['Asignacion']==1)]['Asignacion'].unique() in np.array([1]) else 0]#1 si es que tiene asignacion
#%%%%Almacenamiento sin asignacion sin venta                    
                        disponible=0
                        proceso='No asignado'
                        lpn_cant_inicial=lpn[2]

                        if lpn[3]=='D':
                            # print('9.9 Caso D: LPN sin venta')
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:#disponible=1: que ya se ha escogido zona de trabajo, disponible=0: que aun no se ha escogido zona de trabajo
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar['C'].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            for categoria_i in secuencia:
                                
                                if lpn[2]==0:
                                    # print('9.12 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.13 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.14 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            # print('9.15 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                        
                                    else:
                                        # print('9.16 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue# cambiar de categoría
                                        
                                elif lpn[2]!=0:
                                        # print('9.17 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                        # Que la caja va a ser extinguida
                                        ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                        ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=False)
                                        ubicaciones.apply(almacenar_ubicacion,axis=1)
                                        
#%%%%Almacenamiento sin asignacion con venta y asignacion                                    
                        elif (lpn[3]+lpn[4] in dic_secuencia_asignar.keys()) and lpn_asignado[0]==1:
                            # print('9.18 Caso asig=1: Con venta con asignacion')
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas

                            # print('9.19',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.20',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.21 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.22 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.23 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            # No se almacenó nada del LPN y se debe registrar como LPN no almacenado
                                            break
                                        else:
                                            # print('9.24 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            # terminar el almacenamiento de la caja con lo que se pudo
                                            
                                    else:
                                        # print('9.25 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                    
                                elif lpn[2]!=0:
                                    # print('9.26 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
#%%%%Almacenamiento sin asignacion con venta y no asignacion                                    
                        elif lpn[3]+lpn[4] in dic_secuencia_asignar.keys() and lpn_asignado[0]==0:
                            # print('9.27 caso Asig=0: Con venta sin asignacion')
                            # Para el caso de un producto que no tiene asignación y si tiene venta
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.28',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.29',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.30 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                # elif mantenedor_sinasignar.at[zona_almacenar,'A']==0:#prueba
                                    # print('9.31 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    # cambiar de zona
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            #LPN no fue almacenado 
                                            # print('9.32 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            # print('9.33 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            
                                    else:
                                        # print('9.34 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                        # terminar el almacenamiento de la caja con lo que se pudo
                                        
                                elif lpn[2]!=0:
                                    # print(proceso)
                                    # print('9.35 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                    else:
                        # print('9.36 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0],'Se buscará espacio en ubicaciones disponibles sin asignar')
                        disponible=1
                        proceso='No asignado'
                        ####
                        # disponible=0
                        lpn_cant_inicial=lpn[2]

                        if lpn[3]=='D':
                            # print('9.9 Caso D: LPN sin venta')
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:#disponible=1: que ya se ha escogido zona de trabajo, disponible=0: que aun no se ha escogido zona de trabajo
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar['C'].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            for categoria_i in secuencia:
                                
                                if lpn[2]==0:
                                    # print('9.12 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.13 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.14 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            # print('9.15 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                        
                                    else:
                                        # print('9.16 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue# cambiar de categoría
                                        
                                elif lpn[2]!=0:
                                        # print('9.17 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                        # Que la caja va a ser extinguida
                                        ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                        ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=False)
                                        ubicaciones.apply(almacenar_ubicacion,axis=1)
                                        
                        elif (lpn[3]+lpn[4] in dic_secuencia_asignar.keys()) and lpn_asignado[0]==1:
                            # print('9.18 Caso asig=1: Con venta con asignacion')
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas

                            # print('9.19',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.20',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.21 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.22 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.23 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            # No se almacenó nada del LPN y se debe registrar como LPN no almacenado
                                            break
                                        else:
                                            # print('9.24 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            # terminar el almacenamiento de la caja con lo que se pudo
                                            
                                    else:
                                        # print('9.25 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                    
                                elif lpn[2]!=0:
                                    # print('9.26 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                        elif lpn[3]+lpn[4] in dic_secuencia_asignar.keys() and lpn_asignado[0]==0:
                            # print('9.27 caso Asig=0: Con venta sin asignacion')
                            # Para el caso de un producto que no tiene asignación y si tiene venta
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.28',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.29',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.30 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                # elif mantenedor_sinasignar.at[zona_almacenar,'A']==0:#prueba
                                    # print('9.31 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    # cambiar de zona
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            #LPN no fue almacenado 
                                            # print('9.32 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            break
                                            # print('9.33 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            
                                    else:
                                        continue
                                        # print('9.34 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        # terminar el almacenamiento de la caja con lo que se pudo
                                        
                                elif lpn[2]!=0:
                                    # print('9.35 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                        ####
                        
                else:
                    # print('9.37 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria asignada')
                    continue
#%%%Proceso de almacenamiento en ubicaciones disponibles con asignacion

            elif lpn[2]!=0:
                # print('9.38 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                proceso='Asignado'
                ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==1)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])].copy()
                ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                ubicaciones.apply(almacenar_ubicacion,axis=1)
#%%LPN asignado con venta actual y con venta histórica             

    else:
        # print('9.39 LPN',lpn,'asignado con venta actual y con venta historica')
        # print(mantenedor_asignar)
        secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
        zona_almacenar=mantenedor_asignar[lpn[3]].idxmax(axis='columns')
        # print('9.40',secuencia,zona_almacenar,lpn[0])
        
        for categoria_i in secuencia:
            # print('9.41',categoria_i)
            
            if lpn[2]==0:
                # print('9.42 LPN',lpn[0],'almacenado completamente')
                break
            elif mantenedor_asignar.at[zona_almacenar,categoria_i]==0:
                # print('9.43 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                
                if categoria_i==secuencia[-1]:
                    
#%%%Proceso de almacenamiento en ubicaciones disponibles sin asignacion                    
                    if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                        # Se debe buscar en espacio disponibles sin asignar porque no queda esapcio asignado
                        proceso='No asignado'
                        disponible=0
                        # print('9.44 LPN',lpn[0],'no almacenado en ubicaciones con asignacion y busca espacio disponible')
                        lpn_asignado=[1 if bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])&(bd_ubicaciones_contenido['Asignacion']==1)]['Asignacion'].unique() in np.array([1]) else 0]#1 si es que tiene asignacion
                        lpn_cant_inicial=lpn[2]
                        
#%%%%Almacenamiento sin asignacion sin venta
                        if lpn[3]=='D':
                            # print('9.47 Caso D: LPN sin venta')
                            # Para el caso de LPN sin venta
                            secuencia=dic_secuencia_sinventa['D']
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar['C'].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.48',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.49',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.50 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.51 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.52 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            # terminar el almacenamiento de la caja con lo que se pudo
                                            break
                                        else:
                                            # print('9.53 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                        
                                    else:
                                        # print('9.54 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue# cambiar de categoría
                                        
                                elif lpn[2]!=0:
                                        # print('9.55 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                        # Que la caja va a ser extinguida
                                        ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                        ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=False)
                                        ubicaciones.apply(almacenar_ubicacion,axis=1)
                                        
#%%%%Almacenamiento sin asignacion con venta y asignacion                             
                        elif (lpn[3]+lpn[4] in dic_secuencia_asignar.keys()) and lpn_asignado[0]==1:
                            # print('9.56 Caso asig=1: Con venta con asignacion')
                            # Para el caso de un producto que si tiene asignación y venta histórica y actual
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.57',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.58',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.59 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.60 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.61 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            # No se almacenó nada del LPN y se debe registrar como LPN no almacenado
                                            break
                                        else:
                                            # print('9.62 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            # terminar el almacenamiento de la caja con lo que se pudo
                                            
                                    else:
                                        # print('9.63 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                    
                                elif lpn[2]!=0:
                                    # print('9.64 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
#%%%%Almacenamiento sin asignacion con venta y no asignacion                              
                        elif lpn[3]+lpn[4] in dic_secuencia_asignar.keys() and lpn_asignado[0]==0:
                            # print('9.65 caso Asig=0: Con venta sin asignacion')
                            # Para el caso de un producto que no tiene asignación y si tiene venta
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.66',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.67',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.68 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.69 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            #LPN no fue almacenado 
                                            # print('9.70 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            break
                                            # print('9.71 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            
                                    else:
                                        # print('9.72 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                        
                                elif lpn[2]!=0:
                                    # print('9.73 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                    else:
#%%%Proceso de almacenamiento en ubicaciones disponibles con asignacion
                        # print('9.74 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0],'Se buscará espacio en ubicaciones disponibles sin asignar dentro de la zona')
                        proceso='No asignado'
                        disponible=1
                        lpn_asignado=[1 if bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])&(bd_ubicaciones_contenido['Asignacion']==1)]['Asignacion'].unique() in np.array([1]) else 0]#1 si es que tiene asignacion
                        lpn_cant_inicial=lpn[2]
                        
                        if lpn[3]=='D':
                            # print('9.77 Caso D: LPN sin venta')
                            secuencia=dic_secuencia_sinventa['D']
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar['C'].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.78',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.79',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.80 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.81 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.82 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            # print('9.83 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                        
                                    else:
                                        # print('9.84 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue# cambiar de categoría
                                        
                                elif lpn[2]!=0:
                                        # print('9.85 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                        # Que la caja va a ser extinguida
                                        ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                        ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=False)
                                        ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                        elif (lpn[3]+lpn[4] in dic_secuencia_asignar.keys()) and lpn_asignado[0]==1:
                            # print('9.86 Caso asig=1: Con venta con asignacion')
                            # Para el caso de un producto que si tiene asignación y venta histórica y actual
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.87',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.88',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.89 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.90 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            # print('9.91 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            # No se almacenó nada del LPN y se debe registrar como LPN no almacenado
                                            break
                                        else:
                                            # print('9.92 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            # terminar el almacenamiento de la caja con lo que se pudo
                                            
                                    else:
                                        # print('9.93 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                    
                                elif lpn[2]!=0:
                                    # print('9.94 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                        elif lpn[3]+lpn[3] in dic_secuencia_asignar.keys() and lpn_asignado[0]==0:
                            # print('9.95 caso Asig=0: Con venta sin asignacion')
                            # Para el caso de un producto que no tiene asignación y si tiene venta
                            secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
                            
                            if disponible==1:
                                zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
                            elif disponible==0:
                                zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
                                
                            # print('9.96',secuencia,zona_almacenar,lpn[0])
                            for categoria_i in secuencia:
                                # print('9.97',categoria_i)
                                
                                if lpn[2]==0:
                                    # print('9.98 LPN',lpn[0],'almacenado completamente')
                                    break
                                elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                                    # print('9.99 No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                                    
                                    if categoria_i==secuencia[-1]:
                                        
                                        if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                                            #LPN no fue almacenado 
                                            # print('9.100 LPN',lpn[0],'no almacenado y destinado a piso 1')
                                            destinar_lpn_piso1()
                                            lpn_no_almacenado()
                                            break
                                        else:
                                            # print('9.101 Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                                            break
                                            
                                    else:
                                        # print('9.102 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                                        continue
                                        # terminar el almacenamiento de la caja con lo que se pudo
                                        
                                elif lpn[2]!=0:
                                    # print('9.103 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                                    
                else:
                    # print('9.104 Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria asignada')
                    continue
                
#%%%Proceso de almacenamiento en ubicaciones disponibles con asignacion
            elif lpn[2]!=0:
                # print('9.105 Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                proceso='Asignado'
                ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==1)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])].copy()
                ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                ubicaciones.apply(almacenar_ubicacion,axis=1)
                
#%%Información de avance
    if (caja/bd_lpn_deplin.index[-1])==0:
        print(str((caja/bd_lpn_deplin.index[-1])*100)+'%','de avance...')
        print('Comienza con éxito el proceso',bd_lpn_deplin.shape[0],'cajas por almacenar')
        print(bd_lpn_deplin[dic_campos_asn['ItemsCaja']].sum(),'unidades por almacenar')
    elif caja==((bd_lpn_deplin.shape[0])//2)+1:
        print(str(round((caja/bd_lpn_deplin.index[-1])*100,2))+'%','de avance...')
        print(bd_lpn_deplin.shape[0]-caja+1,'cajas por almacenar')
        print(bd_lpn_deplin[dic_campos_asn['ItemsCaja']][caja:bd_lpn_deplin.shape[0]].sum(),'unidades por almacenar')
    elif (caja/bd_lpn_deplin.index[-1])>0 and caja==(bd_lpn_deplin.index[-1]):
        print(str((caja/bd_lpn_deplin.index[-1])*100)+'%','de avance')
        print(bd_lpn_deplin.shape[0]-(caja+1),'cajas por almacenar')
        # print(bd_lpn_deplin[dic_campos_asn['ItemsCaja']][caja:bd_lpn_deplin.shape[0]].sum(),'unidades por almacenar')
        print(0,'unidades por almacenar. Proceso terminado')
        
#%%Registro de tiempo final
    if caja==bd_lpn_deplin.index[-1]:
        final=time.time()
        diferencia=datetime.datetime.fromtimestamp(final-inicio)
        minutos_1=diferencia.minute
        segundos_1=diferencia.second
        print('Proceso terminado en {minutos} minutos y {segundos} segundos'.format(minutos=minutos_1,segundos=segundos_1))
        print('Hora de termino:',datetime.datetime.now().strftime("%H:%M:%S"))
 
print((final-inicio)/60)
#%%
def almacenamiento_disponible(proceso,disponible):
    '''Función que se encarga de almacenar un LPN en espacios disponibles sin asignar'''
    global lpn
    global zona_almacenar
    global disponible
    global proceso
    global bd_ubicaciones_contenido
    global categoria_i
    global dic_secuencia_sinventa
    global dic_campos_ubicaciones
    global dic_campos_asigna
    global mantenedor_sinasignar
    global destinar_lpn_piso1
    global lpn_no_almacenado
    # global mantenedor_sinasignar
    
    # lpn=list(bd_lpn_deplin.loc[caja])
    lpn_asignado=[1 if bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['CodigoSKU']]==lpn[1])&(bd_ubicaciones_contenido['Asignacion']==1)]['Asignacion'].unique() in np.array([1]) else 0]#1 si es que tiene asignacion
    print('almacenamiento_disponible')
    print(lpn,lpn_asignado)
    lpn_cant_inicial=lpn[2]
    if lpn[3]=='D':
        print('Caso D: LPN sin venta')
        # Para el caso de LPN sin venta
        # se requiere que el almacenamiento siga esta secuencia de búsqueda en las categorías de venta {C-B-A}
        secuencia=dic_secuencia_sinventa['D']
        if disponible==1:
            zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
        elif disponible==0:
            zona_almacenar=mantenedor_sinasignar['C'].idxmax(axis='columns')#Primera ocurrencia en las zonas
            
        print(secuencia,zona_almacenar,lpn[0])
        for categoria_i in secuencia:
            print(categoria_i)
            if lpn[2]==0:
                print('LPN',lpn[0],'almacenado completamente')
                break
            elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                print('No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                if categoria_i==secuencia[-1]:
                    if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                        print('LPN',lpn[0],'no almacenado y destinado a piso 1')
                        # append_lpndestino=(lpn[0],dic_salidas_wcs['EC01'])#datos para el wcs template 
                        # append_lpndestino=pd.DataFrame(data=append_lpndestino,index=columnas_lpndestino).T#datos para el wms template 
                        # wcs_lpndestino=pd.concat([wcs_lpndestino,append_lpndestino])
                        # wcs_lpndestino.drop_duplicates(keep='first',inplace=True)
                        destinar_lpn_piso1()
                        lpn_no_almacenado()
                        # terminar el almacenamiento de la caja con lo que se pudo
                        # enviar LPN al piso 1 de mezzanine
                        break
                    else:
                        print('Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                        break
                else:
                    print('Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                    continue# cambiar de categoría
            elif lpn[2]!=0:
                    print('Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                    # Que la caja va a ser extinguida
                    ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                    ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=False)
                    ubicaciones.apply(almacenar_ubicacion,axis=1)
                # Se extingue la caja
    elif (lpn[3]+lpn[4] in dic_secuencia_asignar.keys()) and lpn_asignado[0]==1:
        print('Caso asig=1: Con venta con asignacion')
        # Para el caso de un producto que si tiene asignación y venta histórica y actual
        secuencia=dic_secuencia_asignar[lpn[3]+lpn[4]]
        if disponible==1:
            zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
        elif disponible==0:
            zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
        # zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')
        print(secuencia,zona_almacenar,lpn[0])
        for categoria_i in secuencia:
            print(categoria_i)
            if lpn[2]==0:
                print('LPN',lpn[0],'almacenado completamente')
                break
            elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                print('No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                if categoria_i==secuencia[-1]:
                    if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                        print('LPN',lpn[0],'no almacenado y destinado a piso 1')
                        destinar_lpn_piso1()
                        lpn_no_almacenado()
                        # No se almacenó nada del LPN y se debe registrar como LPN no almacenado
                        break
                    else:
                        print('Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                        break
                        # terminar el almacenamiento de la caja con lo que se pudo
                else:
                    print('Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                    continue
                # cambiar de categoría
            elif lpn[2]!=0:
                print('Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                ubicaciones.apply(almacenar_ubicacion,axis=1)
                
    elif lpn[3]+lpn[3] in dic_secuencia_asignar.keys() and lpn_asignado[0]==0:
        print('caso Asig=0: Con venta sin asignacion')
        # Para el caso de un producto que no tiene asignación y si tiene venta
        secuencia=dic_secuencia_asignar[lpn[3]+lpn[3]]
        if disponible==1:
            zona_almacenar=zona_almacenar#Primera ocurrencia en las zonas
        elif disponible==0:
            zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')#Primera ocurrencia en las zonas
        # zona_almacenar=mantenedor_sinasignar[lpn[3]].idxmax(axis='columns')
        print(secuencia,zona_almacenar,lpn[0])
        for categoria_i in secuencia:
            print(categoria_i)
            if lpn[2]==0:
                print('LPN',lpn[0],'almacenado completamente')
                break
            elif mantenedor_sinasignar.at[zona_almacenar,categoria_i]==0:
                print('No se encuentran espacios en la zona',zona_almacenar,'categoria',categoria_i)
                # cambiar de zona
                if categoria_i==secuencia[-1]:
                    if lpn[2]==lpn_cant_inicial and categoria_i==secuencia[-1]:
                        #LPN no fue almacenado 
                        print('LPN',lpn[0],'no almacenado y destinado a piso 1')
                        destinar_lpn_piso1()
                        lpn_no_almacenado()
                    else:
                        print('Faltan',lpn_cant_inicial-lpn[2],'unidades por almacenar del LPN',lpn[0])
                else:
                    print('Zona',zona_almacenar,'-Categoria',categoria_i,'sin espacio de almacenamiento. Siguiente categoria')
                    # terminar el almacenamiento de la caja con lo que se pudo
            elif lpn[2]!=0:
                print('Se almacenarán unidades del LPN',lpn[0],'en',zona_almacenar,'-',categoria_i)
                ubicaciones=bd_ubicaciones_contenido[(bd_ubicaciones_contenido[dic_campos_ubicaciones['Categoria']]==categoria_i)&(bd_ubicaciones_contenido['Asignacion']==0)&(bd_ubicaciones_contenido[dic_campos_ubicaciones['Zona']]==zona_almacenar)].copy()
                ubicaciones.sort_values(by=dic_campos_ubicaciones['CodigoUbic'],inplace=True,ascending=True)
                ubicaciones.apply(almacenar_ubicacion,axis=1)
def almacenamiento_asignado():
    '''Función que se encarga de almacenar un LPN en espacios disponibles asignados'''
for i in range(10):
    if i==2:
        print('2')
    else:
        print(i)
        print('test')

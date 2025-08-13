UPDATE `{tabla_temp}` 
        SET indicemd5 = MD5(indice) 
        WHERE 1=1;
    
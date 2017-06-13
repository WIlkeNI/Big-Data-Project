   COMANDOS UTILIZADOS
   
  para arrancar...
     hdfs dfs -ls
     ./hadoopAction.sh start
   
  para compilar el proyecto...
     javac src/trabajo_practico/*.java -classpath /home/hduser/jarHadoop/commons-cli-1.2.jar:/home/hduser/jarHadoop/hadoop-common-2.6.0.jar:/home/hduser/jarHadoop/hadoop-mapreduce-client-core-2.6.0.jar -d bin/
     jar -cvf ./bin/tp/trabajoPractico.jar -C ./bin/ .
     hadoop jar ./bin/tp/trabajoPractico.jar tp.Main ventas empleados
  
  
  ---------------------------------------------------------
  PASOS PARA EJECUTAR SCRIPT DE PIG
  ---------------------------------------------------------
  
  1) Modificar la primera linea del script donde se carga el archivo de logVentas.txt, el mismo tiene que ser un path absoluto o relativo desde donde se ejecuta
  2) Ejecutar 'pig -x local scriptInicial.pig' ---> scriptInicial.pig es el archivo que contiene el codigo que se va a ejecutar
  3) Esperar y tomarse unos matecitos :).

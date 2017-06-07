   COMANDOS UTILIZADOS
   
  para arrancar...
     hdfs dfs -ls
     ./hadoopAction.sh start
   
  para compilar el proyecto...
     javac src/trabajo_practico/*.java -classpath /home/hduser/jarHadoop/commons-cli-1.2.jar:/home/hduser/jarHadoop/hadoop-common-2.6.0.jar:/home/hduser/jarHadoop/hadoop-mapreduce-client-core-2.6.0.jar -d bin/
     jar -cvf ./bin/tp/trabajoPractico.jar -C ./bin/ .
     hadoop jar ./bin/tp/trabajoPractico.jar tp.Main ventas empleados
  

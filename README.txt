   COMANDOS UTILIZADOS
   
    1  hdfs dfs -ls
    4  ./hadoopAction.sh start
   19  grep "^8111" ventas/*.txt >> salida_del_grep.txt
   32  javac src/trabajo_practico/*.java -classpath /home/hduser/jarHadoop/commons-cli-1.2.jar:/home/hduser/jarHadoop/hadoop-common-2.6.0.jar:/home/hduser/jarHadoop/hadoop-mapreduce-client-core-2.6.0.jar -d bin/
   35  jar -cvf ./bin/tp/trabajoPractico.jar -C ./bin/ .
   37  hadoop jar ./bin/tp/trabajoPractico.jar tp.Main ventas

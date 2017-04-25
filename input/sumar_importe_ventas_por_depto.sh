#!/bin/bash

listado=`cat ../output/resumen_ventas/part-r-00000`
contador=0
count=0

if [ $# == 0 ]
then
	echo "**************************************************"
	echo "falta que pases el numero de depto como parametro"
	echo "**************************************************"
	exit 1
fi

echo "sumando..."

for i in $listado; do

	if [ $i == $1 -o $count == 1 ]; then
		let count=count+1
	elif [ $count == 2 ]; then
		contador=$(echo $contador + $i | bc)
		count=0
	fi
	
done

echo "La suma total para el depto: "$1" es: "$contador
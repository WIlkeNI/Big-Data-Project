#!/bin/bash

listado=`grep "^$1" ventas/*.txt | cut -f3`
contador=0

for i in $listado; do
	contador=$(echo $contador + $i | bc)
done

echo $contador

logVentas = LOAD '/media/sf_bigDataProyectos/input/logVentas.txt' AS (idUsuario:int, idProducto:int, tiempo:int, compro:chararray, idSigProducto:int);

#ejercicio 1
filterExerciseOne = FILTER logVentas BY ((idUsuario != 14434) AND (idProducto == 12615 AND tiempo >= 30)) OR (compro == 'true');

STORE filterExerciseOne INTO 'puntoUno';

#guardo los id de los usuarios
usuariosFromFilterOne = FOREACH filterExerciseOne GENERATE idUsuario;

#joineo los id de los usuarios con el logVentas inicial
ventasDeUsuariosFiltrados = JOIN usuariosFromFilterOne BY $0, logVentas BY $0;
ventasDeUsuariosFiltrados = GROUP ventasDeUsuariosFiltrados BY $0;

#ranking por cantidad de compras
topPurchasesRank = ;

#ranking por cantidad de tiempo de visita
topSurfingTimeRank = FOREACH ventasDeUsuariosFiltrados GENERATE group, COUNT(ventasDeUsuariosFiltrados.$2);

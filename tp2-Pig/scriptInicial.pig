
logVentas = LOAD '/media/sf_bigDataProyectos/input/logVentas.txt' AS (idUsuario:int, idProducto:int, tiempo:int, compro:chararray, idSigProducto:int);

----------------------------------------------------------------------------------------
/* PUNTO 1 */
----------------------------------------------------------------------------------------
filterExerciseOne = FILTER logVentas BY ((idUsuario != 14434) AND (idProducto == 12615 AND tiempo >= 30)) OR (compro == 'True');

rmf puntoUno;
STORE filterExerciseOne INTO 'puntoUno';

----------------------------------------------------------------------------------------
/* PUNTO 2 */
----------------------------------------------------------------------------------------
/*guardo los id de los usuarios*/
usuariosFromFilterOne = FOREACH filterExerciseOne GENERATE idUsuario;

/*joineo los id de los usuarios con el logVentas inicial*/
ventasDeUsuariosFiltrados = JOIN usuariosFromFilterOne BY $0, logVentas BY $0;
ventasDeUsuariosFiltrados = GROUP ventasDeUsuariosFiltrados BY $0;
--dump ventasDeUsuariosFiltrados;

/*ranking por cantidad de compras*/
-- usuariosFiltradosConVisitas = FOREACH ventasDeUsuariosFiltrados GENERATE group, ventasDeUsuariosFiltrados.$4 AS compras;

-- topPurchasesRank = ORDER topPurchasesRank BY $1 DESC;

-- rmf rankPorCompras;
-- STORE comprasPorUsuario INTO 'rankPorCompras';

/*ranking por cantidad de tiempo de visita*/
topSurfingTimeRank = FOREACH ventasDeUsuariosFiltrados GENERATE group, SUM(ventasDeUsuariosFiltrados.tiempo);

/* Order by time spent diving the web */
/* $0 -> idUsuario, $1 -> tiempoTotal */
topSurfingTimeRank = ORDER topSurfingTimeRank BY $1 DESC;

rmf rankPorTiempo;
STORE topSurfingTimeRank INTO 'rankPorTiempo';

----------------------------------------------------------------------------------------
/* PUNTO 3 */
----------------------------------------------------------------------------------------

usuariosTop20TimeRank = LIMIT topSurfingTimeRank 20;
usuariosTop20TimeRank = FOREACH usuariosTop20TimeRank GENERATE $0 AS idUsuario;

productosVisitadosPorTop20 = JOIN usuariosTop20TimeRank BY $0, logVentas BY $0;
productosVisitadosPorTop20 = FILTER productosVisitadosPorTop20 BY (tiempo > 10);
productosVisitadosPorTop20 = GROUP productosVisitadosPorTop20 BY $0;
productosVisitadosPorTop20 = FOREACH productosVisitadosPorTop20 GENERATE FLATTEN(productosVisitadosPorTop20.idProducto) AS idProducto;
productosVisitadosPorTop20 = GROUP productosVisitadosPorTop20 BY $0;
productosVisitadosPorTop20 = FOREACH productosVisitadosPorTop20 GENERATE $0;

rmf listadoProductosVisitados;
STORE productosVisitadosPorTop20 INTO 'listadoProductosVisitados';

----------------------------------------------------------------------------------------
/* PUNTO 4 */
----------------------------------------------------------------------------------------

topProductosVisitados = JOIN productosVisitadosPorTop20 BY $0, logVentas BY $1;
topProductosVisitados = FILTER topProductosVisitados BY (compro == 'True');
topProductosVisitados = FOREACH topProductosVisitados GENERATE idProducto, compro;
topProductosVisitados = GROUP topProductosVisitados BY $0;
topProductosVisitados = FOREACH topProductosVisitados GENERATE $0, SIZE(topProductosVisitados.$1);
topProductosVisitados = ORDER topProductosVisitados BY $1 DESC;

rmf listadoPorCantidadDeVentas;
STORE topProductosVisitados INTO 'listadoPorCantidadDeVentas';

----------------------------------------------------------------------------------------
/* PUNTO 5 */
----------------------------------------------------------------------------------------

top4ProductosVisitados = LIMIT topProductosVisitados 4;

rmf top4Productos;
STORE top4ProductosVisitados INTO 'top4Productos';

----------------------------------------------------------------------------------------
/* PUNTO 6 */
----------------------------------------------------------------------------------------
set default_parallel 1

joinTop4LogVentas = JOIN top4ProductosVisitados BY $0, logVentas BY $1;
joinTop4LogVentas = LIMIT joinTop4LogVentas 1;
prodSigMasVisitado = FOREACH joinTop4LogVentas GENERATE idSigProducto, (compro == 'True'? 1 : 0);
top5RecomendacionesA = UNION top4ProductosVisitados, prodSigMasVisitado;
top5RecomendacionesB = GROUP top5RecomendacionesA BY 1;
top5RecomendacionesC = FOREACH top5RecomendacionesB GENERATE FLATTEN(top5RecomendacionesA);
top5RecomendacionesC = FOREACH top5RecomendacionesC GENERATE $0;

rmf recomendaciones;
STORE top5RecomendacionesC INTO 'recomendaciones';

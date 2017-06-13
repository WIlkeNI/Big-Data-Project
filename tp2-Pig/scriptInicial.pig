
logVentas = LOAD '/media/sf_Compartida/Big-Data-Project/input/logVentas.txt' AS (idUsuario:int, idProducto:int, tiempo:int, compro:chararray, idSigProducto:int);

----------------------------------------------------------------------------------------
/* PUNTO 1 */
----------------------------------------------------------------------------------------
filterExerciseOne = FILTER logVentas BY ((idUsuario != 14434) AND (idProducto == 12615 AND tiempo >= 30)) OR ((idProducto == 12615)AND (compro == 'True'));

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
usuariosFiltradosConVisitas = FOREACH ventasDeUsuariosFiltrados GENERATE group, FLATTEN(ventasDeUsuariosFiltrados.$4) AS compras;
usuariosFiltradosConVisitas = FILTER usuariosFiltradosConVisitas BY compras == 'True';

groupedComprasPorUsuario = GROUP usuariosFiltradosConVisitas BY group;
topPurchasesRank = FOREACH groupedComprasPorUsuario GENERATE $0, SIZE($1);
topPurchasesRank = ORDER topPurchasesRank BY $1 DESC;

rmf rankPorCompras;
STORE topPurchasesRank INTO 'rankPorCompras';

/*ranking por cantidad de tiempo de visita*/
topSurfingTimeRank = FOREACH ventasDeUsuariosFiltrados GENERATE group, SUM(ventasDeUsuariosFiltrados.tiempo);

/* Order by time spent diving the web */
/* $0 -> idUsuario, $1 -> tiempoTotal */
topSurfingTimeRank = ORDER topSurfingTimeRank BY $1 DESC;
topComprasYTiempoRank = JOIN topSurfingTimeRank BY $0, topPurchasesRank BY $0;
topComprasYTiempoRank = ORDER topComprasYTiempoRank BY $3 DESC, $1 DESC;

rmf rankPorComprasYTiempo;
STORE topComprasYTiempoRank INTO 'rankPorComprasYTiempo';

----------------------------------------------------------------------------------------
/* PUNTO 3 */
----------------------------------------------------------------------------------------

usuariosTop20TimeRank = LIMIT topComprasYTiempoRank 20;
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
topProductosVisitados = ORDER topProductosVisitados BY $1 DESC, $0 ASC;

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

joinTop4LogVentas = LIMIT top4ProductosVisitados 1;
joinTop4LogVentas = JOIN joinTop4LogVentas BY $0, logVentas BY $1;
joinTop4LogVentas = GROUP joinTop4LogVentas BY $6;
joinTop4LogVentas = FOREACH joinTop4LogVentas GENERATE group, SUM(joinTop4LogVentas.tiempo);
joinTop4LogVentas = ORDER joinTop4LogVentas BY $1 DESC;
joinTop4LogVentas = LIMIT joinTop4LogVentas 1;

rmf recomendaciones;
STORE joinTop4LogVentas INTO 'recomendaciones';

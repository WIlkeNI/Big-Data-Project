import java.util.Properties;
import cascading.util.*;
import cascading.operation.buffer.FirstNBuffer;
import cascading.operation.filter.Limit;
import cascading.operation.*;
import cascading.pipe.joiner.InnerJoin;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
import cascading.operation.aggregator.*;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.CoGroup;
import cascading.pipe.Every;
import cascading.pipe.Merge;
import cascading.pipe.assembly.Unique;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.property.AppProps;
import cascading.scheme.Scheme;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import cascading.operation.expression.ExpressionFilter;
import java.util.*;


public class CascadingWC {

	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPathPunto1 = args[1];
		String outputPathPunto2 = args[2];
		String outputPathPunto3 = args[3];
		String outputPathPunto4 = args[4];
		String outputPathPunto5 = args[5];
		String outputPathPunto6 = args[6];
		int idUsuario = Integer.parseInt(args[7]);
		int idProducto = Integer.parseInt(args[8]);

		Class[] tipos1 = new Class[]{int.class, int.class, int.class, boolean.class, int.class};
		Class[] tipos2 = new Class[]{int.class, int.class, int.class };	
				
		Class[] tiposProductos = new Class[]{int.class, int.class, int.class};
		Class[] tipos3 = new Class[]{int.class, int.class};
		Class[] tipos4 = new Class[]{int.class};

		Scheme sourceScheme = new TextDelimited( new Fields("idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto" ), true, "\t", tipos1 );
		Tap source = new FileTap( sourceScheme, inputPath );

		Scheme sinkSchem = new TextDelimited( new Fields("idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto" ), true, "\t", tipos1 );
		Tap sink = new FileTap(sinkSchem, outputPathPunto1, SinkMode.REPLACE);				
		
		Scheme sinkSchemJoined = new TextDelimited( new Fields("idUsuario", "tiempo", "compro"), true, "\t", tipos2);
		Tap sinkJoined = new FileTap(sinkSchemJoined, outputPathPunto2, SinkMode.REPLACE);				
		
		Scheme sinkSchemProductos = new TextDelimited( new Fields("idProducto", "compro", "tiempo" ), true, "\t", tipos2);
		Tap sinkProductos = new FileTap(sinkSchemProductos, outputPathPunto3, SinkMode.REPLACE);

		Scheme sinkSchemProductosVendidos = new TextDelimited( new Fields("idProducto", "compro" ), true, "\t", tipos3);
		Tap sinkProductosVendidos = new FileTap(sinkSchemProductosVendidos, outputPathPunto4, SinkMode.REPLACE);

		Scheme sinkSchemProductosTop4 = new TextDelimited( new Fields("idProducto"), true, "\t", tipos4);
		Tap sinkProductosTop4 = new FileTap(sinkSchemProductosTop4, outputPathPunto5, SinkMode.REPLACE);		

		Scheme sinkSchemSiguienteProducto = new TextDelimited( new Fields("idSiguienteProducto", "tiempo" ), true, "\t", tipos3);
		Tap sinkSiguienteProducto = new FileTap(sinkSchemSiguienteProducto, outputPathPunto6, SinkMode.REPLACE);
		
		//Pipe que almacena todo el archivo y sirve como auxiliar.
		Pipe assemblyBase = new Pipe("assemblyBase");
		
		//***************************************** BEGIN PUNTO 1 ***************************************** //
		//Dentro de este pipe se desarrolla el punto 1: Todos los usuarios que visitaron el idProducto recibido por parámetro por mas de 30 segundos o que lo compraron
		Pipe assemblyPrincipal = new Pipe( "assemblyPrincipal" );		
		//Filtro que elimina los registros que no coincidan con el producto recibido como parámetro
		ExpressionFilter filtroProducto = new ExpressionFilter( "idProducto != " + idProducto, Double.class );			
		//Nos quedamos con los registros que coincidan con el producto recibido
		assemblyPrincipal = new Each( assemblyPrincipal, new Fields( "idProducto" ), filtroProducto );		
		//Filtro que elimina los registros del usuario recibido como parámetro
		ExpressionFilter filtroUsuario = new ExpressionFilter( "idUsuario == " + idUsuario, Double.class );		
		//Eliminamos de los registros al usuario recibido
		assemblyPrincipal = new Each( assemblyPrincipal, new Fields( "idUsuario" ), filtroUsuario );		
		//Dentro de este pipe vamos a guardar los registros donde se haya comprado el producto
		Pipe assemblyCompro = new Pipe("compro");
		ExpressionFilter filtroCompras = new ExpressionFilter( "compro == false", boolean.class );
		assemblyCompro = new Each(assemblyPrincipal, new Fields("compro"), filtroCompras);		
		//Dentro de este pipe vamos a guardar los registros donde el tiempo es mayor a 30 segundos
		Pipe assemblyTiempoMayorA30 = new Pipe("tiempoMayorA30");
		ExpressionFilter filtroTiempo = new ExpressionFilter( "tiempo < 30", Double.class );
		assemblyTiempoMayorA30 = new Each( assemblyPrincipal, new Fields( "tiempo" ), filtroTiempo );		
		Pipe[] pipes = new Pipe[2];
		pipes[0] = assemblyTiempoMayorA30;
		pipes[1] = assemblyCompro;		
		//Se hace el Join entre los dos pipes
		assemblyPrincipal = new GroupBy(pipes);
		//Se eliminan los repetidos
		assemblyPrincipal = new Unique(assemblyPrincipal, Fields.ALL);
		//***************************************** END PUNTO 1 ***************************************** //
		
		//***************************************** BEGIN PUNTO 2 ***************************************** //
		//Se hace el Join entre los usuarios del punto 1 y el assemblyBase (posee todo el archivo)
		Fields common = new Fields( "idUsuario" );
		Fields declared = new Fields( "idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto", "idUsuario2", "idProducto2", "tiempo2", "compro2", "idSiguienteProducto2" );
		//Dentro de este pipe se desarrolla el punto 2: se rankean los usuarios del punto 1 de acuerdo a las compras que realizaron y al tiempo total de visita en las mismas
		Pipe joined = new CoGroup(assemblyBase, common, assemblyPrincipal, common, declared, new InnerJoin());		
		//Nos quedamos solo con los usuarios que realizaron compras
		joined = new Each(joined, new Fields("compro"), filtroCompras);
		//Agrupamos por usuario
		joined = new GroupBy(joined, new Fields("idUsuario"));
		//Contamos la cantidad de compras
		Aggregator count = new Count( new Fields( "compro" ) );
		joined = new Every(joined, new Fields("compro"), count);
		//Contamos la cantidad total de tiempo que visitó en sus compras
		Aggregator tiempoTotal = new Sum( new Fields( "tiempo" ) );
		joined = new Every(joined, new Fields("tiempo"), tiempoTotal);		
		//Definimos primer filtro: Cantidad de compras (Descendente)
		Fields groupFields = new Fields("compro");
		groupFields.setComparators(Collections.reverseOrder());
		//Definimos segundo filtro: Cantidad de tiempo total en sus compras (Descendente)
		Fields sortFields = new Fields("tiempo");
		sortFields.setComparators(Collections.reverseOrder());		
		//Ordenamos las tuplas
		joined = new GroupBy(joined, groupFields, sortFields);
		//***************************************** END PUNTO 2 ***************************************** //
		
		//***************************************** BEGIN PUNTO 3 ***************************************** //
		//Nos quedamos con el top 20 de lo usuarios del punto 2
		Filter limitUsuarios = new Limit(20);
		Pipe top20Usuarios = new Each(joined, limitUsuarios);
		//Se hace el Join entre el top20 de usuarios y el assemblyBase (posee todo el archivo)
		common = new Fields( "idUsuario" );
		declared = new Fields( "idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto", "idUsuario2", "tiempo2", "compro2");
		//Dentro de este pipe se desarrolla el punto 3: se obtiene el top20 de los usuarios del punto 2 y se obtienen los productos que visitaron por al menos 10 segundos
		Pipe productos = new CoGroup(assemblyBase, common, top20Usuarios, common, declared, new InnerJoin());
		//Se filtran aquellos productos que se hayan visitando al menos 10 segundos
		ExpressionFilter filterTime = new ExpressionFilter( "tiempo < 10", int.class );
		productos = new Each(productos, new Fields("tiempo"), filterTime);
		//***************************************** END PUNTO 3 ***************************************** //
		
		//***************************************** BEGIN PUNTO 4 ***************************************** //
		//Dentro de este pipe se desarrolla el punto 4: Se rankean los productos del punto 3 de acuerdo a su cantidad de ventas
		Pipe productosComprados = new Pipe("productosComprados", productos);
		productosComprados = new Each(productosComprados, new Fields("compro"), filtroCompras);		
		//Se agrupan las tuplas por producto
		productosComprados = new GroupBy(productosComprados, new Fields("idProducto"));
		//Contamos la cantidad de compras
		productosComprados = new Every(productosComprados, new Fields("compro"), count);
		//Filtramos por: Cantidad de compras (Descendente)
		productosComprados = new GroupBy(productosComprados, groupFields);
		//***************************************** END PUNTO 4 ***************************************** //
		
		//***************************************** BEGIN PUNTO 5 ***************************************** //
		//Nos quedamos con el top 4 de los productos
		Filter limit = new Limit(4);
		//Dentro de este pipe se desarrolla el punto 5: Se toma el top 4 de los productos del punto 4
		Pipe top4Productos = new Pipe("top4Productos", productosComprados);		
		top4Productos = new Each(top4Productos, limit);
		//***************************************** END PUNTO 5 ***************************************** //
		
		//***************************************** BEGIN PUNTO 6 ***************************************** //
		//Dentro de este pipe se desarrolla el punto 6: Se toma el top 1 de los productos del punto 5 y se busca el siguiente producto mas visitado
		Pipe productoNumero1 = new Pipe("productoNumero1", top4Productos);
		productoNumero1 = new Each(productoNumero1, new Limit(1));
		//Se hace el Join entre el producto más vendido y el assemblyBase (posee todo el archivo)
		common = new Fields( "idProducto" );
		declared = new Fields( "idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto", "idProducto2", "compro2");
		Pipe joinedProductoMasVendido = new CoGroup(assemblyBase, common, productoNumero1, common, declared, new InnerJoin());
		joinedProductoMasVendido = new GroupBy(joinedProductoMasVendido, new Fields("idSiguienteProducto"));
		//Contamos la cantidad de tiempo que fue visitaado
		joinedProductoMasVendido = new Every(joinedProductoMasVendido, new Fields("tiempo"), tiempoTotal);
		//Definimos filtro: Cantidad de tiempo visitado (Descendente)
		groupFields = new Fields("tiempo");
		groupFields.setComparators(Collections.reverseOrder());
		joinedProductoMasVendido = new GroupBy(joinedProductoMasVendido, groupFields);
		joinedProductoMasVendido = new Each(joinedProductoMasVendido, new Limit(1));
		//***************************************** END PUNTO 6 ***************************************** //

		Properties properties = AppProps.appProps()
				  .setName( "word-count-application" )
				  .setJarClass( CascadingWC.class )
				  .buildProperties();

		FlowConnector fc = new LocalFlowConnector( properties );

		FlowDef flowDef = new FlowDef().setName( "Ejemplo" )
		.addSource( assemblyPrincipal, source )
		.addSource( assemblyBase, source )		
		.addTailSink(assemblyPrincipal, sink)
		.addTailSink(joined, sinkJoined )
		.addTailSink(productos, sinkProductos )
		.addTailSink(productosComprados, sinkProductosVendidos )
		.addTailSink(top4Productos, sinkProductosTop4 )
		.addTailSink(joinedProductoMasVendido, sinkSiguienteProducto );

		Flow flow = fc.connect(flowDef);

		flow.complete();

	}

}

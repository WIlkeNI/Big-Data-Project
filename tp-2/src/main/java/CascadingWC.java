import java.util.Properties;
import cascading.pipe.joiner.InnerJoin;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.aggregator.Sum;
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
		String outputPathJoined = args[2];

		Class[] tipos = new Class[]{int.class, int.class, int.class, boolean.class, int.class};

		Scheme sourceScheme = new TextDelimited( new Fields("idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto" ), true, "\t", tipos );

		Tap source = new FileTap( sourceScheme, inputPath );

		Scheme sinkSchem = new TextDelimited( new Fields("idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto" ), true, "\t", tipos );

		Tap sink = new FileTap(sinkSchem, outputPathPunto1, SinkMode.REPLACE);

		Class[] tiposJoined = new Class[]{int.class, int.class, int.class };		
		
		Scheme sinkSchemJoined = new TextDelimited( new Fields("idUsuario", "tiempo", "compro"), true, "\t", tiposJoined);

		Tap sinkJoined = new FileTap(sinkSchemJoined, outputPathJoined, SinkMode.REPLACE);
		
		Pipe assemblyPrincipal = new Pipe( "buscarUser" );
		
		//Pipe que almacena todo el archivo y sirve como auxiliar.
		Pipe assemblyBase = new Pipe("base");	
		
		ExpressionFilter filtroProducto = new ExpressionFilter( "idProducto != 12615", Double.class );	
		
		//Nos quedamos con los registros que coincidan con el producto recibido
		assemblyPrincipal = new Each( assemblyPrincipal, new Fields( "idProducto" ), filtroProducto );

		ExpressionFilter filtroUsuario = new ExpressionFilter( "idUsuario == 14434", Double.class );
		
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
		
		//Se hace el Join entre los usuarios del punto 1 y el assemblyBase (posee todo el archivo)
		Fields common = new Fields( "idUsuario" );
		Fields declared = new Fields( "idUsuario", "idProducto", "tiempo", "compro", "idSiguienteProducto", "idUsuario2", "idProducto2", "tiempo2", "compro2", "idSiguienteProducto2" );
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

		Properties properties = AppProps.appProps()
				  .setName( "word-count-application" )
				  .setJarClass( CascadingWC.class )
				  .buildProperties();

		FlowConnector fc = new LocalFlowConnector( properties );
		//Flow flow = fc.connect( "word-count", source, sink, assemblyPrincipal );

		FlowDef flowDef = new FlowDef().setName( "Ejemplo" )
		.addSource( assemblyPrincipal, source )
		.addSource( assemblyBase, source )		
		.addTailSink(assemblyPrincipal, sink)
		.addTailSink(joined, sinkJoined );

		Flow flow = fc.connect(flowDef);

		flow.complete();

	}

}

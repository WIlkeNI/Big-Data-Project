import java.util.Properties;
import cascading.pipe.joiner.InnerJoin;
import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
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
		String outputPath = args[1];

		Class[] tipos = new Class[]{int.class, int.class,
			int.class, boolean.class, int.class};

		Scheme sourceScheme = new TextDelimited( new Fields(
			"idUsuario", "idProducto", "tiempo", "compro",
			"idSiguienteProducto" ), true, "\t", tipos );

		// Scheme sourceScheme = new TextLine( new Fields( "line" ) );

		Tap source = new FileTap( sourceScheme, inputPath );

		Scheme sinkSchem = new TextDelimited( new Fields(
			"idUsuario", "idProducto", "tiempo", "compro",
			"idSiguienteProducto" ), true, "\t", tipos );

		Tap sink = new FileTap(sinkSchem, outputPath, SinkMode.REPLACE);
		
		Pipe assemblyPrincipal = new Pipe( "buscarUser" );
		Pipe assemblyBase = new Pipe("base", assemblyPrincipal);
		
		ExpressionFilter filtroProducto = new ExpressionFilter( "idProducto != 12615", Double.class );	

		assemblyPrincipal = new Each( assemblyPrincipal, new Fields( "idProducto" ), filtroProducto );

		ExpressionFilter filtroUsuario = new ExpressionFilter( "idUsuario == 14434",
		Double.class );

		assemblyPrincipal = new Each( assemblyPrincipal, new Fields( "idUsuario" ), filtroUsuario );
		
		Pipe assemblyCompro = new Pipe("compro");

		ExpressionFilter filtroCompras = new ExpressionFilter( "compro == false",
		boolean.class );

		assemblyCompro = new Each(assemblyPrincipal, new Fields("compro"), filtroCompras);

 		// Filtrar el archivo por el id del producto y que el id del usuario no sea el pasado por parametro.
		
		Pipe assemblyTiempoMayorA30 = new Pipe("tiempoMayorA30");

		ExpressionFilter filtroTiempo = new ExpressionFilter( "tiempo < 30", Double.class );

		assemblyTiempoMayorA30 = new Each( assemblyPrincipal, new Fields( "tiempo" ), filtroTiempo );
		
		Pipe[] pipes = new Pipe[2];
		pipes[0] = assemblyTiempoMayorA30;
		pipes[1] = assemblyCompro;		

		assemblyPrincipal = new GroupBy(pipes);

		assemblyPrincipal = new Unique(assemblyPrincipal, Fields.ALL);

		Properties properties = AppProps.appProps()
				  .setName( "word-count-application" )
				  .setJarClass( CascadingWC.class )
				  .buildProperties();

		FlowConnector fc = new LocalFlowConnector( properties );
		Flow flow = fc.connect( "word-count", source, sink, assemblyPrincipal );

		flow.complete();

	}

}

import java.util.Properties;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Aggregator;
import cascading.operation.Function;
import cascading.operation.aggregator.Count;
import cascading.operation.regex.RegexGenerator;
import cascading.pipe.Each;
import cascading.pipe.Every;
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


public class CascadingWC {

	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];

		Class[] tipos = new Class[]{Double.class, Double.class,
			Double.class, boolean.class, Double.class};

		Scheme sourceScheme = new TextDelimited( new Fields(
			"idUsuario", "idProducto", "tiempo", "compro",
			"idSiguienteProducto" ), true, "\t", tipos );

		// Scheme sourceScheme = new TextLine( new Fields( "line" ) );

		Tap source = new FileTap( sourceScheme, inputPath );

		Scheme sinkSchem = new TextDelimited( new Fields(
			"idUsuario", "idProducto", "tiempo", "compro",
			"idSiguienteProducto" ), true, "\t", tipos );

		Tap sink = new FileTap(sinkSchem, outputPath, SinkMode.REPLACE);

		Pipe assembly = new Pipe( "buscarUser" );

		ExpressionFilter filtroProducto = new ExpressionFilter( "idProducto != 12615",
		Double.class );
		

		assembly = new Each( assembly, new Fields( "idProducto" ), filtroProducto );

		ExpressionFilter filtroUsuario = new ExpressionFilter( "idUsuario == 14434",
		Double.class );

		assembly = new Each( assembly, new Fields( "idUsuario" ), filtroUsuario );

 // Filtrar el archivo por el id del producto y que el id del usuario no sea el pasado por parametro.


		ExpressionFilter filtroTiempo = new ExpressionFilter( "tiempo < 30",
		Double.class );

		assembly = new Each( assembly, new Fields( "tiempo" ), filtroTiempo );

		/* = new GroupBy( assembly, new Fields("idProducto"));

		Aggregator count = new Count( new Fields( "count" ) );
		assembly = new Every( assembly, count );*/

		Properties properties = AppProps.appProps()
				  .setName( "word-count-application" )
				  .setJarClass( CascadingWC.class )
				  .buildProperties();

		FlowConnector fc = new LocalFlowConnector( properties );
		Flow flow = fc.connect( "word-count", source, sink, assembly );

		flow.complete();

	}

}

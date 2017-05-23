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


public class CascadingWC {

	public static void main(String[] args) {
		String inputPath = args[0];
		String outputPath = args[1];

		Class[] tipos = new Class[]{double.class, double.class,
			double.class, boolean.class, double.class};

		Scheme sourceScheme = new TextDelimited( new Fields(
			"idUsuario", "idProducto", "tiempo", "compro",
			"idSiguienteProducto" ), true, "\t", tipos );

		// Scheme sourceScheme = new TextLine( new Fields( "line" ) );

		Tap source = new FileTap( sourceScheme, inputPath );

		Scheme skSchem = new TextDelimited(new Fields( "word", "count"));

		Tap sink = new FileTap(skSchem, outputPath, SinkMode.REPLACE);

		Pipe assembly = new Pipe( "wordcount" );

		String regex = "(?<!\\pL)(?=\\pL)[^ ]*(?<=\\pL)(?!\\pL)";
		Function function = new RegexGenerator(new Fields( "word" ), regex);
		assembly = new Each( assembly, new Fields( "line" ), function );

		assembly = new GroupBy( assembly, new Fields( "word" ) );

		Aggregator count = new Count( new Fields( "count" ) );
		assembly = new Every( assembly, count );

		Properties properties = AppProps.appProps()
				  .setName( "word-count-application" )
				  .setJarClass( CascadingWC.class )
				  .buildProperties();

		FlowConnector fc = new LocalFlowConnector( properties );
		Flow flow = fc.connect( "word-count", source, sink, assembly );

		flow.complete();

	}

}

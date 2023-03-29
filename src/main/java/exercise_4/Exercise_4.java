package exercise_4;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.graphframes.GraphFrame;
import static org.apache.spark.sql.functions.desc;

public class Exercise_4 {
	
	public static void wikipedia(JavaSparkContext ctx, SQLContext sqlCtx) {

		//Reading vertex file and initializing JavaRDD row by using lambda function
		JavaRDD<String> verticesRDD = ctx.textFile("src/main/resources/wiki-vertices.txt");
		JavaRDD<Row> verticesRowRDD = verticesRDD.map(line -> {
			String[] parts = line.split("\t");
			//Data type of id column is set to Long since it's too big for Integer Data type
			return RowFactory.create(Long.parseLong(parts[0]), parts[1]);
		});
		//schema: two columns in csv file
		StructType verticesSchema = new StructType(new StructField[]{
				new StructField("id", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("title", DataTypes.StringType, true, new MetadataBuilder().build())
		});
		Dataset<Row> vertices = sqlCtx.createDataFrame(verticesRowRDD, verticesSchema);

		//Reading edge file in and using lambda function to initialize JavaRDD row.
		JavaRDD<String> edgesRDD = ctx.textFile("src/main/resources/wiki-edges.txt");
		JavaRDD<Row> edgesRowRDD = edgesRDD.map(line -> {
			String[] parts = line.split("\t");
			return RowFactory.create(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
		});
		//edge schema: source_id and destination_id of the vertices
		StructType edgesSchema = new StructType(new StructField[]{
				new StructField("src", DataTypes.LongType, true, new MetadataBuilder().build()),
				new StructField("dst", DataTypes.LongType, true, new MetadataBuilder().build())
		});
		Dataset<Row> edges = sqlCtx.createDataFrame(edgesRowRDD, edgesSchema);

		GraphFrame gf = GraphFrame.apply(vertices, edges);
		//calculating pageRank by setting damping factor to 0.7 and max no of iterations to 5
		GraphFrame results = gf.pageRank().resetProbability(0.5).maxIter(10).run();
		//sorting and limiting the result and then displaying the results
		results.vertices().orderBy(desc("pagerank")).limit(10).select("id","title","pagerank").show();
	}
	
}

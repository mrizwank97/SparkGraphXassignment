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

		//calculating pageRank by iterating over damping factor and maxIter hyper parameters of pageRank algorithM
		//damping factor is iterated from 0.95 to 0 with step size of 0.05
		//maxIter parameter is iterated from 5 to 50 with step size of 5
		//for each value of maxIter the damping factor is varied from 0.95 to zero
		for (int i = 1; i <= 20; i++) {
			for (int j = 5; j <= 50; j+=5) {

				Long start = System.currentTimeMillis();
				GraphFrame results = gf.pageRank().resetProbability(1 - (i * 0.05)).maxIter(j).run();
				Long end = System.currentTimeMillis();

				System.out.println("resetProbability: '" + (1 - (i * 0.05)) + "' maxIter: '" + j + "' time: '" + (end - start) + "' msec\n");
				results.vertices().orderBy(desc("pagerank")).limit(10).select("id", "title", "pagerank").show();
			}
		}
	}
}

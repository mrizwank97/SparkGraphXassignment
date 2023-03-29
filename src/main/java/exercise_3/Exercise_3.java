package exercise_3;

        import com.google.common.collect.ImmutableMap;
        import com.google.common.collect.Lists;
        import org.apache.spark.api.java.JavaRDD;
        import org.apache.spark.api.java.JavaSparkContext;
        import org.apache.spark.graphx.*;
        import org.apache.spark.storage.StorageLevel;
        import scala.Tuple2;
        import scala.collection.Iterator;
        import scala.collection.JavaConverters;
        import scala.reflect.ClassTag$;
        import scala.runtime.AbstractFunction1;
        import scala.runtime.AbstractFunction2;
        import scala.runtime.AbstractFunction3;

        import java.io.Serializable;
        import java.util.*;


public class Exercise_3  {

    // Vertex Program implementation
    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>> implements Serializable {
        @Override
        public Tuple2<Integer,List<Object>> apply(Long vertexID, Tuple2<Integer,List<Object>> vertexValue, Tuple2<Integer,List<Object>> message) {

            if (vertexValue._1 <= message._1) {  // if the current vertex value is less than or equal to the incoming message value
                return vertexValue; // keep the current vertex value
            }
            else {
                return message; // else update the vertex value with the incoming message value
            }
        }
    }



    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer,List<Object>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer,List<Object>>>>> implements Serializable {
        @Override
        // The triplet has values of ((SourceVertexID, (SourceVertexValue, SourceVertexPath)), (DstVertexID, (DstVertexValue, DstVertexPath), (EdgeValue))

        public Iterator<Tuple2<Object,Tuple2<Integer,List<Object>>>> apply(EdgeTriplet<Tuple2<Integer,List<Object>>, Integer> triplet) {

            Tuple2<Object,Tuple2<Integer,List<Object>>> sourceVertex = triplet.toTuple()._1(); // the source vertex
            Integer sourceVertexCurrentDistance = sourceVertex._2._1; // the current distance of the source vertex
            List sourceVertexShortestPath = sourceVertex._2._2; // the current shortest path to the source vertex

            Tuple2<Object,Tuple2<Integer,List<Object>>> dstVertex = triplet.toTuple()._2(); // the destination vertex
            Integer dstVertexCurrentDistance = dstVertex._2._1; // the current distance of the destination vertex

            Integer edgeDistance = triplet.toTuple()._3(); // the distance of the edge

            // this if statement required, otherwise the MAX_VALUE is increased by edge distance and becomes negative.
            if  (sourceVertexCurrentDistance == Integer.MAX_VALUE) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>().iterator()).asScala();
                // return an empty iterator if the current source vertex distance is Integer.MAX_VALUE
            }

            else if (sourceVertexCurrentDistance + edgeDistance <= dstVertexCurrentDistance) {
                // if the sum of the source vertex distance and the edge distance is less than or equal to the current distance of the destination vertex

                List<Object> shortestPath = sourceVertexShortestPath;
                shortestPath.add(Long.parseLong(String.valueOf(dstVertex._1)));

                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer,List<Object>>>(triplet.dstId(), new Tuple2<Integer,List<Object>>(sourceVertexCurrentDistance + edgeDistance, shortestPath))).iterator()).asScala();

            } else {
                // do nothing
                System.out.println("2. sendMsg ELSE: sourceVertex._2 (" + sourceVertex._2 + ") + edgeDistance (" + edgeDistance + ") <= dstVertex._2(" + dstVertex._2 + ")");
                System.out.println("2. Return array: " + new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>()); // copy the current shortest path of the source vertex
                System.out.println(""); // add the destination vertex

                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Tuple2<Integer,List<Object>>>>().iterator()).asScala();

            }
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>,Tuple2<Integer,List<Object>>> implements Serializable {
        @Override

        public Tuple2<Integer, List<Object>> apply(Tuple2<Integer, List<Object>> o, Tuple2<Integer, List<Object>> o2) {

            // If the key of the first tuple is less than or equal to the key of the second tuple,
            // return the first tuple
            if (o._1 <= o2._1) {
                return o;
            }
            // Otherwise, return the second tuple
            else {
                return o2;
            }
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();


        List<Tuple2<Object,Tuple2<Integer,List<Object>>>> vertices = Lists.newArrayList(
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(1l, new Tuple2<Integer,List<Object>>(0, Lists.newArrayList(1l))),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(2l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(3l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(4l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(5l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList())),
                new Tuple2<Object,Tuple2<Integer,List<Object>>>(6l, new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList()))
        );


        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l,2l, 4), // A --> B (4)
                new Edge<Integer>(1l,3l, 2), // A --> C (2)
                new Edge<Integer>(2l,3l, 5), // B --> C (5)
                new Edge<Integer>(2l,4l, 10), // B --> D (10)
                new Edge<Integer>(3l,5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );


        // Through all of the following code, we have to update our class to match the new VD
        JavaRDD<Tuple2<Object,Tuple2<Integer,List<Object>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);


        Graph<Tuple2<Integer,List<Object>>,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),new Tuple2<Integer,List<Object>>(0, Lists.newArrayList(1l)), StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));


        ops.pregel(new Tuple2<Integer,List<Object>>(Integer.MAX_VALUE, Lists.newArrayList()), // initialMsg
                        Integer.MAX_VALUE,      // maxIterations
                        EdgeDirection.Out(),    //activeDirection
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))      // VD classTag
                .vertices()
                .toJavaRDD()
                .foreach(v -> {

                    Tuple2<Object,Tuple2<Integer,List<Object>>> vertex = (Tuple2<Object,Tuple2<Integer,List<Object>>>)v;
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2._2+" with cost "+vertex._2._1);
                });
    }

}
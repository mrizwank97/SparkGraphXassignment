package exercise_2;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class Exercise_2 {

    // Function to update vertex value based on the received message
    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
        @Override
        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
            // Return the minimum value between the current vertex value and the received message
            return Math.min(vertexValue, message);
        }
    }

    // Function to send messages along edges
    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
            // Extract source, destination vertices and edge distance
            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();
            Integer edgeDistance = triplet.toTuple()._3();

            // If the source vertex value is Integer.MAX_VALUE, return an empty iterator (no message sent)
            if  (sourceVertex._2 == Integer.MAX_VALUE) {
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
            }

            // If the sum of source vertex value and edge distance is less than or equal to the destination vertex value,
            // send a message with the new value to the destination vertex
            else if (sourceVertex._2 + edgeDistance <= dstVertex._2) {
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2 + edgeDistance)).iterator()).asScala();

            } else {

                // Otherwise, return an empty iterator (no message sent)
                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();

            }
        }
    }

    // Function to merge messages sent to a vertex
    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
        @Override

        // Return null (not used in this implementation)
        public Integer apply(Integer o, Integer o2) {

            return null;
        }
    }

    // Function to find the shortest path using Pregel API
    public static void shortestPaths(JavaSparkContext ctx) {
        // Map vertex IDs to labels for readability
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();


        // Initialize vertices with values and edges with distances
        // The graph is represented using two lists: vertices and edges
        // The first vertex is the starting vertex, and its distance is 0.
        // All other vertices are initialized with distance "infinity" represented by Integer.MAX_VALUE

        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
                new Tuple2<Object,Integer>(1l,0),
                new Tuple2<Object,Integer>(2l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(3l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(4l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(5l,Integer.MAX_VALUE),
                new Tuple2<Object,Integer>(6l,Integer.MAX_VALUE)
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

        // Convert the vertex and edge lists to RDDs (Resilient Distributed Datasets)
        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        // Create a graph from the RDDs
        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // Create a GraphOps object, which provides operations on graphs
        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        // Use the Pregel API to calculate the shortest path between vertices
        ops.pregel(Integer.MAX_VALUE,
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Integer.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {

                    Tuple2<Object,Integer> vertex = (Tuple2<Object,Integer>)v;
                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2);
                });
    }

}
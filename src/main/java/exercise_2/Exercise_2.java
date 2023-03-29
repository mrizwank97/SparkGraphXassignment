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


//public class Exercise_2 {
//    // Here we will override the function called "apply" that is part of VProg. Everything inside the {} after the parameters will be
//    // what apply is replace with.
//    private static class VProg extends AbstractFunction3<Long,Integer,Integer,Integer> implements Serializable {
//        @Override
//        public Integer apply(Long vertexID, Integer vertexValue, Integer message) {
//            // This is the comparison that needs to happen at each vertex.
//            // we will compare the vertex to the incoming message and return either
//            // the current vertex value OR the incoming message. We are essentially
//            // replacing the current vertex value with a new one.
////            System.out.println("1. VProg: vertexID: " + vertexID + ". vertexValue: " + vertexValue + ". message: " + message);
//            return Math.min(vertexValue, message);
//        }
//    }
//
//    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Integer,Integer>, Iterator<Tuple2<Object,Integer>>> implements Serializable {
//        @Override
//        // The triplet has values of ((SourceVertexID, SourceVertexValue), (DstVertexID, DstVertexValue), (EdgeValue))
//        // So here I apply the function EdgeTriplet and the result will be an iterable Tuple that contains the vertexID
//        // that needs to be updated along with the new value.
//        // Compare the attr (edgeValue) to the vertex value. If the sum of triplet._2 (source vertexValue) + attr (edgeValue)
//        // is less than triplet._2 (dest vertexValue), then the message will be a vertex value with the new sum.
//        // otherwise, return (empty array)
//        public Iterator<Tuple2<Object, Integer>> apply(EdgeTriplet<Integer, Integer> triplet) {
//            Tuple2<Object,Integer> sourceVertex = triplet.toTuple()._1();
//            Tuple2<Object,Integer> dstVertex = triplet.toTuple()._2();
//            Integer edgeDistance = triplet.toTuple()._3();
//
//            // this if statement required, otherwise the MAX_VALUE is increased by edge distance and becomes negative.
//            if  (sourceVertex._2 == Integer.MAX_VALUE) {
//                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
//            }
//
//            else if (sourceVertex._2 + edgeDistance <= dstVertex._2) {    // sourceVertex + edge distance less than dstVertex?
////                System.out.println("2. sendMsg IF: sourceVertex._2 (" + sourceVertex._2 + ") + edgeDistance (" + edgeDistance + ") <= dstVertex._2(" + dstVertex._2 + ")");
////                System.out.println("2. Return array: " + Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2 + edgeDistance)));
////                System.out.println("");
//                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Integer>(triplet.dstId(),sourceVertex._2 + edgeDistance)).iterator()).asScala();
//
//            } else {
//                // do nothing
////                System.out.println("2. sendMsg ELSE: sourceVertex._2 (" + sourceVertex._2 + ") + edgeDistance (" + edgeDistance + ") <= dstVertex._2(" + dstVertex._2 + ")");
////                System.out.println("2. Return array: " + new ArrayList<Tuple2<Object,Integer>>());
////                System.out.println("");
//                // TODO: why do I need to return an empty array? Is this so that the Iterator can continue "moving" through my object?
//                return JavaConverters.asScalaIteratorConverter(new ArrayList<Tuple2<Object,Integer>>().iterator()).asScala();
//
//            }
//        }
//    }
//
//    private static class merge extends AbstractFunction2<Integer,Integer,Integer> implements Serializable {
//        @Override
//        // TODO: Confirm that we only compare 2 values because this is called after every edge is compared using sendMsg.
//        public Integer apply(Integer o, Integer o2) {
//            // if a vertex receives 2 messages, it will choose the smallest of the 2 values.
////            System.out.println("3. merge: o" + o + ". o2: " + o2 + ". return: " + Math.min(o, o2));
////            System.out.println("");
////            return Math.min(o, o2);
////            System.out.println("MERGE!");
//            return null;
//        }
//    }
//
//    public static void shortestPaths(JavaSparkContext ctx) {
//        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
//                .put(1l, "A")
//                .put(2l, "B")
//                .put(3l, "C")
//                .put(4l, "D")
//                .put(5l, "E")
//                .put(6l, "F")
//                .build();
//
//
//        // TODO: Would it make sense to initialize to Double.PositiveInfinity instead of integer with MAX_VALUE to simplify
//        //  the sendMsg class and remove the overflow issue?
//        List<Tuple2<Object,Integer>> vertices = Lists.newArrayList(
//                new Tuple2<Object,Integer>(1l,0),
//                new Tuple2<Object,Integer>(2l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(3l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(4l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(5l,Integer.MAX_VALUE),
//                new Tuple2<Object,Integer>(6l,Integer.MAX_VALUE)
//        );
//        List<Edge<Integer>> edges = Lists.newArrayList(
//                new Edge<Integer>(1l,2l, 4), // A --> B (4)
//                new Edge<Integer>(1l,3l, 2), // A --> C (2)
//                new Edge<Integer>(2l,3l, 5), // B --> C (5)
//                new Edge<Integer>(2l,4l, 10), // B --> D (10)
//                new Edge<Integer>(3l,5l, 3), // C --> E (3)
//                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
//                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
//        );
//        // Creating object called verticesRDD of type JavaRDD with a parameter Tuple2.
//        // JavaRDD creates a distributed collection of objects, which means they can be
//        // parallelized across a cluster of nodes.
//        // TODO: The ctx.parallelize creates a parallelized version of the java object? and JavaRDD is scala?
//        // Tuple2 is an object that contains a fixed number (2) of elements. So here we
//        // have a tuple containing an object in position _1 and an integer in position _2.
//        JavaRDD<Tuple2<Object,Integer>> verticesRDD = ctx.parallelize(vertices);
//        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);
//
//        // Here we create an object "G" of type Graph. The Graph class looks like Graph[VD,ED], and here we are saying that the
//        // VD (vertexes) and ED (edges) will both be of type integer. The angle brackets are used to pass a parameter as a
//        // generic type.
//        // TODO: So why can't we just say Graph G = Graph.apply(...)?
//        // TODO: Why do I need to use the .apply() function? Why isn't it Graph G = new Graph(parameters...)?
//        // Anyway, now we have G, a Graph with vertexes and edges that are parallelized.
//        Graph<Integer,Integer> G = Graph.apply(verticesRDD.rdd(),edgesRDD.rdd(),1, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
//                scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
//
//        // GraphOps is an extension of Graphs, and it includes two additional class tags. From documentation:
//        // "ClassTag[T] stores the erased class of a given type T , accessible via the runtimeClass field.
//        // This is particularly useful for instantiating Array s whose element types are unknown at compile time."
//        // TODO: What is a class tag?
//        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Integer.class),scala.reflect.ClassTag$.MODULE$.apply(Integer.class));
//
//        // Now we call pregel and pass all of our needed parameters to get it running.
//        // TODO: What exactly is the difference between an API and a function? they feel the same.
//        ops.pregel(Integer.MAX_VALUE,
//                        Integer.MAX_VALUE,
//                        EdgeDirection.Out(),
//                        new VProg(),
//                        new sendMsg(),
//                        new merge(),
//                        ClassTag$.MODULE$.apply(Integer.class))
//                .vertices()
//                .toJavaRDD()
//                .foreach(v -> {
//                    // similar to for loop in python, v will be each element we iterate. But in Java, we need to declare all variables
//                    // so we cast v to vertex.
//                    Tuple2<Object,Integer> vertex = (Tuple2<Object,Integer>)v;
//                    System.out.println("Minimum cost to get from "+labels.get(1l)+" to "+labels.get(vertex._1)+" is "+vertex._2);
//                });
//    }
//
//}





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
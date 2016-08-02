package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.common.model.api.epgm.Edge;
import org.gradoop.common.model.api.epgm.GraphHead;
import org.gradoop.common.model.api.epgm.Vertex;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;

import java.util.Collection;

public class GradoopFlinkTestUtils {

  public static <T> T writeAndRead(T element) throws Exception {
    return writeAndRead(element, ExecutionEnvironment.getExecutionEnvironment());
  }

  public static <T> T writeAndRead(T element, ExecutionEnvironment env)
    throws Exception {
    DataSet<T> dataSet = env.fromElements(element);
    return dataSet.collect().get(0);
  }

  public static void printLogicalGraph(LogicalGraph graph) throws
    Exception {
    Collection<GraphHead> graphHeadCollection = Lists.newArrayList();
    Collection<Vertex> vertexCollection = Lists.newArrayList();
    Collection<Edge> edgeCollection = Lists.newArrayList();

    graph.getGraphHead().output(
      new LocalCollectionOutputFormat<>(graphHeadCollection));
    graph.getVertices().output(
      new LocalCollectionOutputFormat<>(vertexCollection));
    graph.getEdges().output(
      new LocalCollectionOutputFormat<>(edgeCollection));

    graph.getConfig().getExecutionEnvironment().execute();

    System.out.println("*** GraphHead Collection ***");
    for (GraphHead g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** Vertex Collection ***");
    for (Vertex v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** Edge Collection ***");
    for (Edge e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static void printGraphCollection(
    GraphCollection collection) throws Exception {

    Collection<GraphHead> graphHeadCollection = Lists.newArrayList();
    Collection<Vertex> vertexCollection = Lists.newArrayList();
    Collection<Edge> edgeCollection = Lists.newArrayList();

    collection.getGraphHeads().output(
      new LocalCollectionOutputFormat<>(graphHeadCollection));
    collection.getVertices().output(
      new LocalCollectionOutputFormat<>(vertexCollection));
    collection.getEdges().output(
      new LocalCollectionOutputFormat<>(edgeCollection));

    collection.getConfig().getExecutionEnvironment().execute();

    System.out.println("*** GraphHead Collection ***");
    for (GraphHead g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** Vertex Collection ***");
    for (Vertex v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** Edge Collection ***");
    for (Edge e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static void printDirectedCanonicalAdjacencyMatrix(
    LogicalGraph graph) throws Exception {

    printDirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static void printDirectedCanonicalAdjacencyMatrix(
    GraphCollection collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), true).execute(collection).print();
  }

  public static void printUndirectedCanonicalAdjacencyMatrix(
    LogicalGraph graph) throws Exception {

    printUndirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static void printUndirectedCanonicalAdjacencyMatrix(
    GraphCollection collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToDataString<>(),
      new VertexToDataString<>(),
      new EdgeToDataString<>(), false).execute(collection).print();
  }
}

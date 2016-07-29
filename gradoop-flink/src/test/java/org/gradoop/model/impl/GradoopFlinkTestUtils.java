package org.gradoop.model.impl;

import com.google.common.collect.Lists;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.model.api.epgm.Edge;
import org.gradoop.model.api.epgm.GraphHead;
import org.gradoop.model.api.epgm.Vertex;
import org.gradoop.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.model.impl.operators.tostring.functions.VertexToDataString;

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

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printLogicalGraph(LogicalGraph<G, V, E> graph) throws
    Exception {
    Collection<G> graphHeadCollection = Lists.newArrayList();
    Collection<V> vertexCollection = Lists.newArrayList();
    Collection<E> edgeCollection = Lists.newArrayList();

    graph.getGraphHead().output(
      new LocalCollectionOutputFormat<>(graphHeadCollection));
    graph.getVertices().output(
      new LocalCollectionOutputFormat<>(vertexCollection));
    graph.getEdges().output(
      new LocalCollectionOutputFormat<>(edgeCollection));

    graph.getConfig().getExecutionEnvironment().execute();

    System.out.println("*** GraphHead Collection ***");
    for (G g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** Vertex Collection ***");
    for (V v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** Edge Collection ***");
    for (E e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printGraphCollection(
    GraphCollection<G, V, E> collection) throws Exception {

    Collection<G> graphHeadCollection = Lists.newArrayList();
    Collection<V> vertexCollection = Lists.newArrayList();
    Collection<E> edgeCollection = Lists.newArrayList();

    collection.getGraphHeads().output(
      new LocalCollectionOutputFormat<>(graphHeadCollection));
    collection.getVertices().output(
      new LocalCollectionOutputFormat<>(vertexCollection));
    collection.getEdges().output(
      new LocalCollectionOutputFormat<>(edgeCollection));

    collection.getConfig().getExecutionEnvironment().execute();

    System.out.println("*** GraphHead Collection ***");
    for (G g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** Vertex Collection ***");
    for (V v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** Edge Collection ***");
    for (E e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printDirectedCanonicalAdjacencyMatrix(
    LogicalGraph<G, V,E> graph) throws Exception {

    printDirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printDirectedCanonicalAdjacencyMatrix(
    GraphCollection<G, V, E> collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder<>(
      new GraphHeadToDataString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), true).execute(collection).print();
  }

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printUndirectedCanonicalAdjacencyMatrix(
    LogicalGraph<G, V,E> graph) throws Exception {

    printUndirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static <
    G extends GraphHead,
    V extends Vertex,
    E extends Edge> void printUndirectedCanonicalAdjacencyMatrix(
    GraphCollection<G, V, E> collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder<>(
      new GraphHeadToDataString<G>(),
      new VertexToDataString<V>(),
      new EdgeToDataString<E>(), false).execute(collection).print();
  }
}

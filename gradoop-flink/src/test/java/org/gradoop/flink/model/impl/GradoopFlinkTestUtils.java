package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.api.entities.EPGMGraphElement;
import org.gradoop.common.model.api.entities.EPGMGraphHead;
import org.gradoop.common.model.api.entities.EPGMVertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCellComparator;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.common.adjacencylist.IdDirection;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class GradoopFlinkTestUtils {

  public static <T> T writeAndRead(T element) throws Exception {
    return writeAndRead(element, ExecutionEnvironment.getExecutionEnvironment());
  }

  public static <T> T writeAndRead(T element, ExecutionEnvironment env)
    throws Exception {
    DataSet<T> dataSet = env.fromElements(element);
    return dataSet.collect().get(0);
  }

  public static void printLogicalGraph(LogicalGraph graph)
    throws Exception {
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

    System.out.println("*** EPGMGraphHead Collection ***");
    for (EPGMGraphHead g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** EPGMVertex Collection ***");
    for (EPGMVertex v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** EPGMEdge Collection ***");
    for (EPGMEdge e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static void printGraphCollection(GraphCollection collection)
    throws Exception {

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

    System.out.println("*** EPGMGraphHead Collection ***");
    for (EPGMGraphHead g : graphHeadCollection) {
      System.out.println(g);
    }

    System.out.println("*** EPGMVertex Collection ***");
    for (EPGMVertex v : vertexCollection) {
      System.out.println(v);
    }

    System.out.println("*** EPGMEdge Collection ***");
    for (EPGMEdge e : edgeCollection) {
      System.out.println(e);
    }
  }

  public static void printDirectedCanonicalAdjacencyMatrix(LogicalGraph graph)
    throws Exception {

    printDirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static void printDirectedCanonicalAdjacencyMatrix(
    GraphCollection collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), true).execute(collection).print();
  }

  public static void printUndirectedCanonicalAdjacencyMatrix(LogicalGraph graph)
    throws Exception {

    printUndirectedCanonicalAdjacencyMatrix(GraphCollection.fromGraph(graph));
  }

  public static void printUndirectedCanonicalAdjacencyMatrix(
    GraphCollection collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), false).execute(collection).print();
  }

  public static void assertEquals(GraphTransaction a, GraphTransaction b) {
    GradoopTestUtils.validateEPGMElements(a.getGraphHead(), b.getGraphHead());
    GradoopTestUtils.validateEPGMElementCollections(a.getVertices(), b.getVertices());
    GradoopTestUtils.validateEPGMElementCollections(a.getEdges(), b.getEdges());
  }

  private static void assertEqualEdges(EPGMEdge a, EPGMEdge b) {
    assertTrue(a.getSourceId().equals(b.getSourceId()));
    assertTrue(a.getTargetId().equals(b.getTargetId()));
    assertEqualGraphElements(a, b);
  }

  private static void assertEqualGraphElements(EPGMGraphElement a, EPGMGraphElement b) {
    assertTrue(a.getGraphIds().equals(b.getGraphIds()));
    assertEqualElements(a, b);
  }

  private static void assertEqualElements(EPGMElement a, EPGMElement b) {
    assertTrue(a.getId().equals(b.getId()));
    assertTrue(a.getLabel().equals(b.getLabel()));
    assertTrue(a.getProperties() == null && b.getProperties() == null ||
      a.getProperties().equals(b.getProperties()));
  }

  public static void assertEquals(
    AdjacencyList<IdDirection, GradoopId> a, AdjacencyList<IdDirection, GradoopId> b) {

    assertTrue(a.getGraphId().equals(b.getGraphId()));

    Set<GradoopId> ids = Sets.newHashSet(a.getGraphId());

    Map<GradoopId, AdjacencyListRow<IdDirection, GradoopId>> aRows = a.getRows();
    Map<GradoopId, AdjacencyListRow<IdDirection, GradoopId>> bRows = b.getRows();

    assertTrue(aRows.size() == bRows.size());

    for (GradoopId vertexId : aRows.keySet()) {
      ids.add(vertexId);

      List<AdjacencyListCell<IdDirection, GradoopId>> aCells = 
        Lists.newArrayList(aRows.get(vertexId).getCells());
      
      List<AdjacencyListCell<IdDirection, GradoopId>> bCells = 
        Lists.newArrayList(aRows.get(vertexId).getCells());

      assertTrue(aCells.size() == bCells.size());

      aCells.sort(new AdjacencyListCellComparator<>());
      bCells.sort(new AdjacencyListCellComparator<>());

      assertTrue(aCells.equals(bCells));

      for (AdjacencyListCell<IdDirection, GradoopId> cell : aCells) {
        if (cell.getEdgeData().isOutgoing()) {
          ids.add(cell.getVertexData());
        }
      }
    }

    for (GradoopId id : ids) {
      assertTrue(a.getLabel(id).equals(b.getLabel(id)));
      Properties aProperties = a.getProperties(id);
      Properties bProperties = b.getProperties(id);

      assertTrue(aProperties == null && bProperties == null 
        || aProperties.equals(bProperties));

    }
  }
}

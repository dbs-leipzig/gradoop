/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.model.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.LocalCollectionOutputFormat;
import org.gradoop.common.GradoopTestUtils;
import org.gradoop.common.model.api.entities.Edge;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.common.model.api.entities.GraphElement;
import org.gradoop.common.model.api.entities.GraphHead;
import org.gradoop.common.model.api.entities.Vertex;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.impl.epgm.GraphCollection;
import org.gradoop.flink.model.impl.epgm.LogicalGraph;
import org.gradoop.flink.model.impl.operators.tostring.CanonicalAdjacencyMatrixBuilder;
import org.gradoop.flink.model.impl.operators.tostring.functions.EdgeToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.GraphHeadToDataString;
import org.gradoop.flink.model.impl.operators.tostring.functions.VertexToDataString;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCell;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListCellComparator;
import org.gradoop.flink.representation.common.adjacencylist.AdjacencyListRow;
import org.gradoop.flink.representation.transactional.AdjacencyList;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.junit.Assert;

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
    Collection<EPGMGraphHead> graphHeadCollection = Lists.newArrayList();
    Collection<EPGMVertex> vertexCollection = Lists.newArrayList();
    Collection<EPGMEdge> edgeCollection = Lists.newArrayList();

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

  public static void printGraphCollection(GraphCollection collection)
    throws Exception {

    Collection<EPGMGraphHead> graphHeadCollection = Lists.newArrayList();
    Collection<EPGMVertex> vertexCollection = Lists.newArrayList();
    Collection<EPGMEdge> edgeCollection = Lists.newArrayList();

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

  public static void printDirectedCanonicalAdjacencyMatrix(LogicalGraph graph)
    throws Exception {

    printDirectedCanonicalAdjacencyMatrix(graph.getCollectionFactory()
      .fromGraph(graph));
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

    printUndirectedCanonicalAdjacencyMatrix(graph.getCollectionFactory()
      .fromGraph(graph));
  }

  public static void printUndirectedCanonicalAdjacencyMatrix(
    GraphCollection collection) throws Exception {

    new CanonicalAdjacencyMatrixBuilder(
      new GraphHeadToDataString(),
      new VertexToDataString(),
      new EdgeToDataString(), false).execute(collection).print();
  }

  public static void assertEquals(GraphTransaction a, GraphTransaction b) {
    GradoopTestUtils.validateElements(a.getGraphHead(), b.getGraphHead());
    GradoopTestUtils.validateElementCollections(a.getVertices(), b.getVertices());
    GradoopTestUtils.validateElementCollections(a.getEdges(), b.getEdges());
  }

  private static void assertEqualEdges(Edge a, Edge b) {
    Assert.assertEquals(a.getSourceId(), b.getSourceId());
    Assert.assertEquals(a.getTargetId(), b.getTargetId());
    assertEqualGraphElements(a, b);
  }

  private static void assertEqualGraphElements(GraphElement a, GraphElement b) {
    Assert.assertEquals(a.getGraphIds(), b.getGraphIds());
    assertEqualElements(a, b);
  }

  private static void assertEqualElements(Element a, Element b) {
    Assert.assertEquals(a.getId(), b.getId());
    Assert.assertEquals(a.getLabel(), b.getLabel());
    assertTrue(a.getProperties() == null && b.getProperties() == null ||
      a.getProperties().equals(b.getProperties()));
  }

  public static void assertEquals(AdjacencyList<GradoopId, String, GradoopId, GradoopId> a,
    AdjacencyList<GradoopId, String, GradoopId, GradoopId> b) {

    assertEqualElements(a.getGraphHead(), b.getGraphHead());

    Set<GradoopId> ids = Sets.newHashSet();

    Map<GradoopId, AdjacencyListRow<GradoopId, GradoopId>> aRows = a.getOutgoingRows();
    Map<GradoopId, AdjacencyListRow<GradoopId, GradoopId>> bRows = b.getOutgoingRows();

    Assert.assertEquals(aRows.size(), bRows.size());

    for (GradoopId vertexId : aRows.keySet()) {
      ids.add(vertexId);

      List<AdjacencyListCell<GradoopId, GradoopId>> aCells =
        Lists.newArrayList(aRows.get(vertexId).getCells());

      List<AdjacencyListCell<GradoopId, GradoopId>> bCells =
        Lists.newArrayList(aRows.get(vertexId).getCells());

      Assert.assertEquals(aCells.size(), bCells.size());

      aCells.sort(new AdjacencyListCellComparator<>());
      bCells.sort(new AdjacencyListCellComparator<>());

      Assert.assertEquals(aCells, bCells);

      for (AdjacencyListCell<GradoopId, GradoopId> cell : aCells) {
        ids.add(cell.getVertexData());
      }
    }

    for (GradoopId id : ids) {
      Assert.assertEquals(a.getLabel(id), b.getLabel(id));
      Properties aProperties = a.getProperties(id);
      Properties bProperties = b.getProperties(id);

      assertTrue(
        aProperties == null && bProperties == null || aProperties.equals(bProperties));

    }
  }
}

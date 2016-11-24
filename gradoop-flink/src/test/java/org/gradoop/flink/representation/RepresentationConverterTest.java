package org.gradoop.flink.representation;

import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpan;
import org.gradoop.flink.model.GradoopFlinkTestBase;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyListNullValueFactory;
import org.gradoop.flink.representation.transactional.adjacencylist.AdjacencyList;
import org.gradoop.flink.representation.transactional.sets.GraphTransaction;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertTrue;

public class RepresentationConverterTest extends GradoopFlinkTestBase {


  @Test
  public void testGraphTransactionAdjacencyList() throws Exception {

    GraphTransaction transaction = getGraphTransaction();

    AdjacencyList<Object> adjacencyList = RepresentationConverters
      .getAdjacencyList(transaction, new AdjacencyListNullValueFactory());

    GraphTransaction convertedTransaction = GSpan
      .getGraphTransaction(adjacencyList);

    AdjacencyList<Object> convertedAdjacencyList = RepresentationConverters
      .getAdjacencyList(convertedTransaction, new AdjacencyListNullValueFactory());

    assertTrue(transaction.equals(convertedTransaction));
    assertTrue(transaction.toString().equals(convertedTransaction.toString()));
    assertTrue(adjacencyList.equals(convertedAdjacencyList));
    assertTrue(adjacencyList.toString().equals(convertedAdjacencyList.toString()));
  }

  private GraphTransaction getGraphTransaction() {
    GraphHead graphHead = new GraphHead(GradoopId.get(), "Test", null);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(graphHead.getId());
    Set<Vertex> vertices = Sets.newHashSet();
    Set<Edge> edges = Sets.newHashSet();

    Properties aProperties = new Properties();
    aProperties.set("x", 1);
    Vertex v1 = new Vertex(GradoopId.get(), "A", aProperties, graphIds);
    Vertex v2 = new Vertex(GradoopId.get(), "B", null, graphIds);

    vertices.add(v1);
    vertices.add(v2);

    Properties loopProperties = new Properties();

    edges.add(
      new Edge(GradoopId.get(), "loop", v1.getId(), v1.getId(), loopProperties, graphIds));
    edges.add(
      new Edge(GradoopId.get(), "m", v1.getId(), v2.getId(), null, graphIds));
    edges.add(
      new Edge(GradoopId.get(), "m", v1.getId(), v2.getId(), null, graphIds));
    edges.add(
      new Edge(GradoopId.get(), "m", v2.getId(), v1.getId(), null, graphIds));

    return new GraphTransaction(graphHead, vertices, edges);
  }

}
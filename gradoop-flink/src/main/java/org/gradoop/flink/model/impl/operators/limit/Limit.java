
package org.gradoop.flink.model.impl.operators.limit;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.api.operators
  .UnaryCollectionToCollectionOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphsContainmentFilterBroadcast;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAllGraphsBroadcast;


import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns the first n (arbitrary) logical graphs from a collection.
 *
 * Note that this operator uses broadcasting to distribute the relevant graph
 * identifiers.
 */
public class Limit implements UnaryCollectionToCollectionOperator {

  /**
   * Number of graphs that are retrieved from the collection.
   */
  private final int limit;

  /**
   * Creates a new limit operator instance.
   *
   * @param limit number of graphs to retrieve from the collection
   */
  public Limit(int limit) {
    this.limit = limit;
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    DataSet<GraphHead> graphHeads = collection.getGraphHeads().first(limit);

    DataSet<GradoopId> firstIds = graphHeads.map(new Id<GraphHead>());

    DataSet<Vertex> filteredVertices = collection.getVertices()
      .filter(new InAllGraphsBroadcast<Vertex>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    DataSet<Edge> filteredEdges = collection.getEdges()
      .filter(new InAllGraphsBroadcast<Edge>())
      .withBroadcastSet(firstIds, GraphsContainmentFilterBroadcast.GRAPH_IDS);

    return GraphCollection.fromDataSets(graphHeads,
      filteredVertices,
      filteredEdges,
      collection.getConfig());
  }

  @Override
  public String getName() {
    return Limit.class.getName();
  }
}

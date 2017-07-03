
package org.gradoop.flink.model.impl.operators.aggregation;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.ElementsOfSelectedGraphs;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.operators.aggregation.functions.ApplyAggregateEdges;
import org.gradoop.flink.model.impl.operators.aggregation.functions.ApplyAggregateVertices;
import org.gradoop.flink.model.impl.operators.aggregation.functions.CombinePartitionApplyAggregates;
import org.gradoop.flink.model.impl.operators.aggregation.functions.SetAggregateProperties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a collection of logical graphs and a user defined aggregate function as
 * input. The aggregate function is applied on each logical graph contained in
 * the collection and the aggregate is stored as an additional property at the
 * graphs.
 */
public class ApplyAggregation
  implements ApplicableUnaryGraphToGraphOperator {

  /**
   * User-defined aggregate function which is applied on a graph collection.
   */
  private final AggregateFunction aggregateFunction;

  /**
   * Creates a new operator instance.
   *
   * @param aggregateFunction     function to compute aggregate value
   */
  public ApplyAggregation(final AggregateFunction aggregateFunction) {
    this.aggregateFunction = checkNotNull(aggregateFunction);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {

    DataSet<GraphHead> graphHeads = collection.getGraphHeads();
    DataSet<Vertex> vertices = collection.getVertices();
    DataSet<Edge> edges = collection.getEdges();

    DataSet<Tuple2<GradoopId, PropertyValue>> aggregate;

    DataSet<GradoopId> graphIds = graphHeads
      .map(new Id<GraphHead>());

    if (this.aggregateFunction instanceof VertexAggregateFunction) {
      aggregate = aggregateVertices(vertices, graphIds);
    } else {
      aggregate = aggregateEdges(edges, graphIds);
    }

    aggregate = aggregate
      .groupBy(0)
      .reduceGroup(new CombinePartitionApplyAggregates(aggregateFunction));

    graphHeads = graphHeads
      .coGroup(aggregate)
      .where(new Id<GraphHead>()).equalTo(0)
      .with(new SetAggregateProperties(aggregateFunction));

    return GraphCollection.fromDataSets(graphHeads,
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig());
  }

  /**
   * Applies an aggregate function to the partitions of a vertex data set.
   *
   * @param vertices vertex data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> aggregateVertices(
    DataSet<Vertex> vertices, DataSet<GradoopId> graphIds) {
    return vertices
      .flatMap(new ElementsOfSelectedGraphs<Vertex>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateVertices(
        (VertexAggregateFunction) aggregateFunction));
  }

  /**
   * Applies an aggregate function to the partitions of an edge data set.
   *
   * @param edges edge data set
   * @param graphIds graph ids to aggregate
   * @return partition aggregate value
   */
  private DataSet<Tuple2<GradoopId, PropertyValue>> aggregateEdges(
    DataSet<Edge> edges, DataSet<GradoopId> graphIds) {
    return edges
      .flatMap(new ElementsOfSelectedGraphs<Edge>())
      .withBroadcastSet(graphIds, ElementsOfSelectedGraphs.GRAPH_IDS)
      .groupBy(0)
      .combineGroup(new ApplyAggregateEdges(
        (EdgeAggregateFunction) aggregateFunction));
  }

  @Override
  public String getName() {
    return ApplyAggregation.class.getName();
  }
}

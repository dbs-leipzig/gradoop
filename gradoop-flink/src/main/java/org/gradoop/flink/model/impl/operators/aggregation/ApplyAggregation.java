/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

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
import org.gradoop.flink.model.api.functions.VertexAndEdgeAggregateFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.GraphElementExpander;
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
    DataSet<Vertex> vertices = collection.getVertices();
    DataSet<Edge> edges = collection.getEdges();

    DataSet<Tuple2<GradoopId, PropertyValue>> aggregateValues;

    if (this.aggregateFunction instanceof VertexAggregateFunction &&
      this.aggregateFunction instanceof EdgeAggregateFunction) {

      VertexAndEdgeAggregateFunction aggregateFunction =
        (VertexAndEdgeAggregateFunction) this.aggregateFunction;

      DataSet<Tuple2<GradoopId, PropertyValue>> vertexAggregateValues = vertices
        .flatMap(new GraphElementExpander<Vertex>())
        .groupBy(0)
        .combineGroup(new ApplyAggregateVertices(aggregateFunction));

      DataSet<Tuple2<GradoopId, PropertyValue>> edgeAggregateValues = edges
        .flatMap(new GraphElementExpander<Edge>())
        .groupBy(0)
        .combineGroup(new ApplyAggregateEdges(aggregateFunction));

      aggregateValues = vertexAggregateValues
        .union(edgeAggregateValues);

    } else if (this.aggregateFunction instanceof VertexAggregateFunction) {

      VertexAggregateFunction aggregateFunction =
        (VertexAggregateFunction) this.aggregateFunction;

      aggregateValues = vertices
        .flatMap(new GraphElementExpander<Vertex>())
        .groupBy(0)
        .combineGroup(new ApplyAggregateVertices(aggregateFunction));

    } else {
      EdgeAggregateFunction aggregateFunction =
        (EdgeAggregateFunction) this.aggregateFunction;

      aggregateValues = edges
        .flatMap(new GraphElementExpander<Edge>())
        .groupBy(0)
        .combineGroup(new ApplyAggregateEdges(aggregateFunction));
    }

    aggregateValues = aggregateValues
      .groupBy(0)
      .reduceGroup(new CombinePartitionApplyAggregates(aggregateFunction));
    
    DataSet<GraphHead> graphHeads = collection.getGraphHeads()
      .coGroup(aggregateValues)
      .where(new Id<GraphHead>()).equalTo(0)
      .with(new SetAggregateProperties(aggregateFunction));

    return GraphCollection.fromDataSets(graphHeads,
      collection.getVertices(),
      collection.getEdges(),
      collection.getConfig());
  }

  @Override
  public String getName() {
    return ApplyAggregation.class.getName();
  }
}

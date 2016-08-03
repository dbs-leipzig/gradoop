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
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.api.functions.ApplyAggregateFunction;
import org.gradoop.flink.model.api.operators.ApplicableUnaryGraphToGraphOperator;
import org.gradoop.flink.model.impl.GraphCollection;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.aggregation.functions.LeftOuterPropertySetter;
import org.gradoop.common.model.impl.properties.PropertyValue;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Takes a collection of logical graphs and a user defined aggregate function as
 * input. The aggregate function is applied on each logical graph contained in
 * the collection and the aggregate is stored as an additional property at the
 * graphs.
 */
public class ApplyAggregation implements ApplicableUnaryGraphToGraphOperator {

  /**
   * Used to store aggregate result.
   */
  private final String aggregatePropertyKey;

  /**
   * User-defined aggregate function which is applied on a graph collection.
   */
  private final ApplyAggregateFunction aggregateFunction;

  /**
   * Creates a new operator instance.
   *
   * @param aggregatePropertyKey  property key to store aggregate value
   * @param aggregateFunction     function to compute aggregate value
   */
  public ApplyAggregation(final String aggregatePropertyKey,
    final ApplyAggregateFunction aggregateFunction) {
    this.aggregatePropertyKey = checkNotNull(aggregatePropertyKey);
    this.aggregateFunction = checkNotNull(aggregateFunction);
  }

  @Override
  public GraphCollection execute(GraphCollection collection) {
    DataSet<Tuple2<GradoopId, PropertyValue>> aggregateValues =
      aggregateFunction.execute(collection);

    DataSet<GraphHead> graphHeads = collection.getGraphHeads()
      .coGroup(aggregateValues)
      .where(new Id<GraphHead>()).equalTo(0)
      .with(new LeftOuterPropertySetter<GraphHead>(
        aggregatePropertyKey,
        PropertyValue.create(aggregateFunction.getDefaultValue())));

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

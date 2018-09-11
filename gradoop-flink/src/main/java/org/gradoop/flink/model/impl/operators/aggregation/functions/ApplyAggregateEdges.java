/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.EdgeAggregateFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * Applies edge aggregate functions to edges with the same graph id.
 * (graphId,edge),.. => (graphId,[aggregateKey,aggregateValue]),..
 */
public class ApplyAggregateEdges implements GroupCombineFunction
  <Tuple2<GradoopId, Edge>, Tuple2<GradoopId, Map<String, PropertyValue>>> {

  /**
   * Aggregate function.
   */
  private final Set<EdgeAggregateFunction> aggregateFunctions;
  /**
   * Reuse tuple.
   */
  private final Tuple2<GradoopId, Map<String, PropertyValue>> reusePair = new Tuple2<>();

  /**
   * Constructor.
   *
   * @param aggregateFunctions aggregate functions
   */
  public ApplyAggregateEdges(Set<EdgeAggregateFunction> aggregateFunctions) {
    this.aggregateFunctions = aggregateFunctions;
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, Edge>> edges,
    Collector<Tuple2<GradoopId, Map<String, PropertyValue>>> out) {

    Iterator<Tuple2<GradoopId, Edge>> iterator = edges.iterator();
    Tuple2<GradoopId, Edge> graphIdEdge = iterator.next();

    Map<String, PropertyValue> aggregate = AggregateUtil.edgeIncrement(new HashMap<>(),
      graphIdEdge.f1, aggregateFunctions);

    while (iterator.hasNext()) {
      Edge edge = iterator.next().f1;
      aggregate = AggregateUtil.edgeIncrement(aggregate, edge, aggregateFunctions);
    }

    if (!aggregate.isEmpty()) {
      reusePair.f0 = graphIdEdge.f0;
      reusePair.f1 = aggregate;
      out.collect(reusePair);
    }
  }
}

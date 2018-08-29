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
/**
 * (graphId,vertex),.. => (graphId,aggregateValue),..
 */
package org.gradoop.flink.model.impl.operators.aggregation.functions;

import org.apache.flink.api.common.functions.GroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.api.functions.VertexAggregateFunction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * (graphId,vertex),.. => (graphId,[aggregateKey,aggregateValue]),..
 */
public class ApplyAggregateVertices implements GroupCombineFunction
  <Tuple2<GradoopId, Vertex>, Tuple2<GradoopId, Map<String, PropertyValue>>> {

  /**
   * Aggregate functions.
   */
  private final Set<VertexAggregateFunction> aggFuncs;
  /**
   * Reuse tuple.
   */
  private final Tuple2<GradoopId, Map<String, PropertyValue>> reusePair = new Tuple2<>();

  /**
   * Constructor.
   *
   * @param aggFuncs aggregate functions
   */
  public ApplyAggregateVertices(Set<VertexAggregateFunction> aggFuncs) {
    this.aggFuncs = aggFuncs;
  }

  @Override
  public void combine(Iterable<Tuple2<GradoopId, Vertex>> vertices,
    Collector<Tuple2<GradoopId, Map<String, PropertyValue>>> out) {

    Iterator<Tuple2<GradoopId, Vertex>> iterator = vertices.iterator();
    Tuple2<GradoopId, Vertex> graphIdVertex = iterator.next();

    Map<String, PropertyValue> aggregate = AggregateUtil.vertexIncrement(new HashMap<>(),
      graphIdVertex.f1, aggFuncs);

    while (iterator.hasNext()) {
      Vertex vertex = iterator.next().f1;
      aggregate = AggregateUtil.vertexIncrement(aggregate, vertex, aggFuncs);
    }

    if (!aggregate.isEmpty()) {
      reusePair.f0 = graphIdVertex.f0;
      reusePair.f1 = aggregate;
      out.collect(reusePair);
    }
  }
}

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
package org.gradoop.flink.model.impl.operators.sampling.statistics.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Collects the aggregated values for connected components and computes the distribution of
 * vertices and edges over this components.
 */
public class GetConnectedComponentDistributionFlatMap implements
  FlatMapFunction<GraphHead, Tuple3<String, Long, Long>> {

  /**
   * Property key to store the component id.
   */
  private final String propertyKey;

  /**
   * Whether to write the component property to the edges.
   */
  private final boolean annotateEdges;

  /**
   * Constructor to initialize function
   *
   * @param propertyKey Property key to store the component id
   * @param annotateEdges Whether to write the component property to the edges
   */
  public GetConnectedComponentDistributionFlatMap(String propertyKey, boolean annotateEdges) {
    this.propertyKey = propertyKey;
    this.annotateEdges = annotateEdges;
  }

  /**
   * Collects the aggregated component ids from vertices and edges. Extracts the distribution for
   * vertices and edges over this components.
   *
   * @param graphHead The graph head with the aggregated components
   * @param out The connected component distribution as {@code Tuple3<String, Long, Long>}
   */
  @Override
  public void flatMap(GraphHead graphHead, Collector<Tuple3<String, Long, Long>> out) {
    List<String> vertexWcc = graphHead.getPropertyValue(
      new AggregateListOfWccVertices(propertyKey).getAggregatePropertyKey()).getList()
      .stream().map(PropertyValue::getString)
      .collect(Collectors.toList());
    Set<String> distinctComponentIds = new HashSet<>(vertexWcc);

    List<String> edgeWcc = new ArrayList<>();
    if (annotateEdges) {
      edgeWcc = graphHead.getPropertyValue(
        new AggregateListOfWccEdges(propertyKey).getAggregatePropertyKey()).getList()
        .stream().map(PropertyValue::getString)
        .collect(Collectors.toList());
    }
    List<String> finalEdgeWcc = edgeWcc;

    List<Tuple3<String, Long, Long>> wccDist = distinctComponentIds.stream()
      .map(wccId -> new Tuple3<>(wccId,
        (long) Collections.frequency(vertexWcc, wccId),
        annotateEdges ? (long) Collections.frequency(finalEdgeWcc, wccId) : -1L))
      .collect(Collectors.toList());

    for (Tuple3<String, Long, Long> distTuple : wccDist) {
      out.collect(distTuple);
    }
  }

}

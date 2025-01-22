/*
 * Copyright © 2014 - 2021 Leipzig University (Database Research Group)
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
package org.gradoop.temporal.model.impl.operators.metric;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.BuildTemporalDegreeTree;
import org.gradoop.temporal.model.impl.operators.metric.functions.FlatMapVertexIdEdgeInterval;
import org.gradoop.temporal.model.impl.operators.metric.functions.TransformDeltaToAbsoluteDegreeTree;

import java.util.Objects;
import java.util.TreeMap;

/**
 * Abstract class as parent for aggregated degree evolution operators.
 */
abstract class BaseAggregateDegreeEvolution
  implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple3<Long, Long, Float>>> {

  /**
   * The time dimension that will be considered.
   */
  private TimeDimension dimension = TimeDimension.VALID_TIME;

  /**
   * The degree type (IN, OUT, BOTH);
   */
  private VertexDegree degreeType = VertexDegree.BOTH;

  /**
   * Default constructor using {@link TimeDimension#VALID_TIME} as default time dimension and
   * {@link VertexDegree#BOTH} as default degree type.
   */
  protected BaseAggregateDegreeEvolution() {
  }

  /**
   * Abstract constructor for the aggregated degree evolution of a graph.
   *
   * @param degreeType the degree type (IN, OUT or BOTH)
   * @param dimension the time dimension to consider (VALID_TIME or TRANSACTION_TIME)
   */
  protected BaseAggregateDegreeEvolution(VertexDegree degreeType, TimeDimension dimension) {
    this.degreeType = Objects.requireNonNull(degreeType);
    this.dimension = Objects.requireNonNull(dimension);
  }

  /**
   * A pre-process function to prevent duplicate code for min, max and avg aggregation. The result is an
   * absolute degree tree for each vertex (id).
   *
   * @param graph the temporal graph as input
   * @return a dataset containing an absolute degree tree for each vertex identifier
   */
  public DataSet<Tuple2<GradoopId, TreeMap<Long, Integer>>> preProcess(TemporalGraph graph) {
    return graph.getEdges()
      // 1) Extract vertex id(s) and corresponding time intervals
      .flatMap(new FlatMapVertexIdEdgeInterval(dimension, degreeType))
      // 2) Group them by the vertex id
      .groupBy(0)
      // 3) For each vertex id, build a degree tree data structure
      .reduceGroup(new BuildTemporalDegreeTree())
      // 4) Transform each tree to aggregated evolution
      .map(new TransformDeltaToAbsoluteDegreeTree());
  }
}

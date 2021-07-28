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

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.BuildTemporalDegreeTree;
import org.gradoop.temporal.model.impl.operators.metric.functions.CalculateDegreesDefaultTimesFlatMap;
import org.gradoop.temporal.model.impl.operators.metric.functions.CalculateDegreesFlatMap;
import org.gradoop.temporal.model.impl.operators.metric.functions.ExtendVertexDataWithInterval;
import org.gradoop.temporal.model.impl.operators.metric.functions.ExtractIdIntervalMap;
import org.gradoop.temporal.model.impl.operators.metric.functions.FlatMapVertexIdEdgeInterval;

import java.util.Objects;
import java.util.TreeMap;

/**
 * A TPGM operator calculating the evolution of vertex degrees for all vertices of the graph. The result is a
 * dataset of tuples {@code {id,t-from,t-to,degree}} where {@code id} is the vertex id, {@code t-from} is the
 * lower interval bound, {@code t-to} the upper interval bound and {@code degree} the degree value.
 * <p>
 * The type of the degree (IN, OUT, BOTH) as well as the time dimension to consider, can be chosen by the
 * arguments.
 * <p>
 * If the vertex degree evolution of a single vertex is needed, use the edge induced subgraph operator
 * {@link org.gradoop.flink.model.api.epgm.BaseGraphOperators#edgeInducedSubgraph(FilterFunction)} with filter
 * {@link org.gradoop.flink.model.impl.functions.epgm.BySourceId} and/or
 * {@link org.gradoop.flink.model.impl.functions.epgm.ByTargetId} before you apply this operator.
 */
public class TemporalVertexDegree
  implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple4<GradoopId, Long, Long, Integer>>> {

  /**
   * The time dimension that will be considered.
   */
  private final TimeDimension dimension;

  /**
   * The degree type (IN, OUT, BOTH);
   */
  private final VertexDegree degreeType;

  /**
   * A flag to decide whether to include the vertex interval as lower and upper bounds for the first and
   * last degree period. If set to false, the first degree period starts at
   * {@link org.gradoop.temporal.model.impl.pojo.TemporalElement#DEFAULT_TIME_FROM} and the last degree period
   * ends at {@link org.gradoop.temporal.model.impl.pojo.TemporalElement#DEFAULT_TIME_TO}.
   */
  private boolean includeVertexTime = false;

  /**
   * Creates an instance of this temporal vertex degree operator.
   *
   * @param degreeType the degree type to consider
   * @param dimension the time dimension to consider
   */
  public TemporalVertexDegree(VertexDegree degreeType, TimeDimension dimension) {
    this.degreeType = Objects.requireNonNull(degreeType);
    this.dimension = Objects.requireNonNull(dimension);
  }

  /**
   * Configure the operator to consider or not the vertex time by calculating the degree evolution.
   *
   * @param includeVertexTime true, iff the vertex time is used as lower and upper bounds for the first and
   *                          last degree period. Note that this requires a join.
   */
  public void setIncludeVertexTime(boolean includeVertexTime) {
    this.includeVertexTime = includeVertexTime;
  }

  @Override
  public DataSet<Tuple4<GradoopId, Long, Long, Integer>> execute(TemporalGraph graph) {

    DataSet<Tuple2<GradoopId, TreeMap<Long, Integer>>> edgesWithTrees = graph.getEdges()
      // 1) Extract vertex id(s) and corresponding time intervals
      .flatMap(new FlatMapVertexIdEdgeInterval(dimension, degreeType))
      // 2) Group them by the vertex id
      .groupBy(0)
      // 3) For each vertex id, build a degree tree data structure
      .reduceGroup(new BuildTemporalDegreeTree());

    if (includeVertexTime) {
      // The time information of the vertex must be included, therefore we need to join.
      DataSet<Tuple3<GradoopId, Long, Long>> vertexIdInterval = graph.getVertices()
        .map(new ExtractIdIntervalMap(dimension));

      return edgesWithTrees
        // 4) Join the vertices to get each vertex interval
        .join(vertexIdInterval)
        .where(0).equalTo(0)
        .with(new ExtendVertexDataWithInterval())
        // 5) Since join leads to possible divided groups, we need to group again
        .groupBy(0)
        .reduce(new BuildTemporalDegreeTree())
        // 6) For each vertex, calculate the degree evolution and output a tuple {v_id, t_from, t_to, degree}
        .flatMap(new CalculateDegreesFlatMap());
    } else {
      return edgesWithTrees
        // 4/7) For each vertex, calculate the degree evolution and output a tuple
        // {v_id, t_from, t_to, degree}
        .flatMap(new CalculateDegreesDefaultTimesFlatMap());
    }
  }
}

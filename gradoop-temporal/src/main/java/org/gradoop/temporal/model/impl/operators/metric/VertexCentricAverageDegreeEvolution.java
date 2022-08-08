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
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.functions.epgm.BySourceId;
import org.gradoop.flink.model.impl.functions.epgm.ByTargetId;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.FilterEdgesInInterval;
import org.gradoop.temporal.model.impl.operators.metric.functions.MapCalculateAverageDegree;
import org.gradoop.temporal.model.impl.operators.metric.functions.MapCalculatePartialAverageDegree;

import java.util.Objects;

/**
 * A TPGM operator which calculates the average degree of a given vertex referenced via its {@code vertexId}
 * within a given time interval: start {@code queryFrom}, end {@code queryTo}. The result is a single
 * value (Double).
 * <p>
 * The type of the degree (IN, OUT, BOTH) can be chosen by the arguments.
 */
public class VertexCentricAverageDegreeEvolution implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Double>>> {
  /**
   * The time dimension that will be considered.
   */
  private final TimeDimension dimension;

  /**
   * The degree type (IN, OUT, BOTH);
   */
  private final VertexDegree degreeType;

  /**
   * The vertex to be considered.
   */
  private final GradoopId vertexId;

  /**
   * The start of the interval specified by the user.
   */
  private final Long queryFrom;
  /**
   * The end of the interval specified by the user.
   */
  private final Long queryTo;

  /**
   * Creates an instance of this temporal average vertex degree operator.
   *
   * @param degreeType the degree type to consider
   * @param dimension  the time dimension to consider
   * @param vertexId   the id of the vertex to consider
   * @param queryFrom  the start of the interval
   * @param queryTo    the end of the interval
   */
  public VertexCentricAverageDegreeEvolution(VertexDegree degreeType, TimeDimension dimension,
                                             GradoopId vertexId, Long queryFrom, Long queryTo) {
    this.degreeType = Objects.requireNonNull(degreeType);
    this.dimension = Objects.requireNonNull(dimension);
    this.vertexId = Objects.requireNonNull(vertexId);
    this.queryFrom = Objects.requireNonNull(queryFrom);
    this.queryTo = Objects.requireNonNull(queryTo);
  }

  @Override
  public DataSet<Tuple1<Double>> execute(TemporalGraph graph) {

    // Find relevant subgraph (vertex and all its edges)
    TemporalGraph subGraph1 = graph.edgeInducedSubgraph(new BySourceId<>(vertexId));
    TemporalGraph subGraph2 = graph.edgeInducedSubgraph(new ByTargetId<>(vertexId));
    TemporalGraph subGraph = subGraph1.combine(subGraph2);
    // Apply TemporalVertexDegree on subgraph
    TemporalVertexDegree temporalVertexDegree = new TemporalVertexDegree(degreeType, dimension);
    temporalVertexDegree.setIncludeVertexTime(true);
    DataSet<Tuple4<GradoopId, Long, Long, Integer>> filteredEdges = temporalVertexDegree.execute(subGraph)
            // Find relevant edges which exist within the given time
            .filter(new FilterEdgesInInterval(queryFrom, queryTo, vertexId));

    return filteredEdges
            // Map each tuple to an interim result from which we can calculate the overall average degree
            .map(new MapCalculatePartialAverageDegree(queryFrom, queryTo))
            // Group dataset and sum all the interim results from before
            .groupBy(0)
              .aggregate(Aggregations.SUM, 1)
              // Now divide this sum by the length of the time interval and return a Tuple1<Double>
              // containing the average degree
              .map(new MapCalculateAverageDegree(queryFrom, queryTo));
  }
}

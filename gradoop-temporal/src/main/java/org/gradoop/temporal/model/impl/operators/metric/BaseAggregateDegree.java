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
import org.gradoop.temporal.model.impl.operators.metric.functions.AggregationType;

import java.util.Objects;

/**
 * An abstract base class which calculates the minimum or maximum degree of a given vertex referenced via its
 * {@code vertexId} within a given time interval: start {@code queryFrom}, end {@code queryTo}. The result is
 * a single value (Integer) in a DataSet. This class has two subclasses for each aggregation type
 * (min and max).
 * <p>
 * The type of the degree (IN, OUT, BOTH) can be chosen by the arguments.
 */
public abstract class BaseAggregateDegree
        implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple1<Integer>>> {
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
   * The type of aggregation to be performed (min or max)
   */
  private final AggregationType aggregationType;

  /**
   * Creates an instance of this temporal vertex degree aggregation operator.
   *
   * @param degreeType      the degree type to consider
   * @param dimension       the time dimension to consider
   * @param vertexId        the id of the vertex to consider
   * @param queryFrom       the start of the interval
   * @param queryTo         the end of the interval
   * @param aggregationType the type of aggregation (min or max)
   */
  public BaseAggregateDegree(VertexDegree degreeType, TimeDimension dimension, GradoopId vertexId,
                             Long queryFrom, Long queryTo, AggregationType aggregationType) {
    this.degreeType = Objects.requireNonNull(degreeType);
    this.dimension = Objects.requireNonNull(dimension);
    this.vertexId = Objects.requireNonNull(vertexId);
    this.queryFrom = Objects.requireNonNull(queryFrom);
    this.queryTo = Objects.requireNonNull(queryTo);
    this.aggregationType = Objects.requireNonNull(aggregationType);
  }

  @Override
  public DataSet<Tuple1<Integer>> execute(TemporalGraph graph) {

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

    switch (aggregationType) {
    case MIN:
      return filteredEdges
              // Group dataset and find minimum degree
              .groupBy(0)
              .aggregate(Aggregations.MIN, 3)
              // get field 3 which contains the minimum degree -> return Tuple1<Integer>
              // containing the minimum degree
              .project(3);
    case MAX:
      return filteredEdges
              // group dataset and find maximum degree
              .groupBy(0)
              .aggregate(Aggregations.MAX, 3)
              // get field 3 which contains the maximum degree -> return Tuple1<Integer>
              // containing the maximum degree
              .project(3);
    default:
      throw new IllegalArgumentException("Aggregate type not specified.");
    }
  }
}

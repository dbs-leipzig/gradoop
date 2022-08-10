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
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.operators.UnaryBaseGraphToValueOperator;
import org.gradoop.flink.model.impl.operators.sampling.functions.VertexDegree;
import org.gradoop.temporal.model.api.TimeDimension;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.metric.functions.GroupDegreeTreesToAggregateDegrees;
import org.gradoop.temporal.model.impl.operators.metric.functions.TransformDeltaToAbsoluteDegreeTree;
import org.gradoop.temporal.model.impl.operators.metric.functions.FlatMapVertexIdEdgeInterval;
import org.gradoop.temporal.model.impl.operators.metric.functions.BuildTemporalDegreeTree;
import org.gradoop.temporal.model.impl.operators.metric.functions.ExtractAllTimePointsReduce;
import org.gradoop.temporal.model.impl.operators.metric.functions.AggregateType;

import java.util.Objects;
import java.util.TreeMap;

/**
 * Operator that calculates the minimum degree evolution of all vertices of a temporal graph for the
 * whole lifetime of the graph.
 */
public class MinDegreeEvolution
    implements UnaryBaseGraphToValueOperator<TemporalGraph, DataSet<Tuple2<Long, Integer>>> {
  /**
   * The time dimension that will be considered.
   */
  private final TimeDimension dimension;

  /**
   * The degree type (IN, OUT, BOTH);
   */
  private final VertexDegree degreeType;

  /**
   * Creates an instance of this minimum degree evolution operator.
   *
   * @param degreeType the degree type to use (IN, OUT, BOTH).
   * @param dimension  the time dimension to use (VALID_TIME, TRANSACTION_TIME).
   *
   * @throws RuntimeException in case of an error.
   */
  public MinDegreeEvolution(VertexDegree degreeType, TimeDimension dimension) throws RuntimeException {
    this.degreeType = Objects.requireNonNull(degreeType);
    this.dimension = Objects.requireNonNull(dimension);
  }

  @Override
  public DataSet<Tuple2<Long, Integer>> execute(TemporalGraph graph) {
    DataSet<Tuple2<GradoopId, TreeMap<Long, Integer>>> absoluteDegreeTrees = graph.getEdges()
        // 1) Extract vertex id(s) and corresponding time intervals
        .flatMap(new FlatMapVertexIdEdgeInterval(dimension, degreeType))
        // 2) Group them by the vertex id
        .groupBy(0)
        // 3) For each vertex id, build a degree tree data structure
        .reduceGroup(new BuildTemporalDegreeTree())
        // 4) Transform each tree to aggregated evolution
        .map(new TransformDeltaToAbsoluteDegreeTree());

    DataSet<Tuple1<Long>> timePoints = absoluteDegreeTrees
        // 5) extract all timestamps where degree of any vertex changes
        .reduceGroup(new ExtractAllTimePointsReduce())
        .distinct();

    return absoluteDegreeTrees
        // 6) Merge trees together and calculate aggregation
        .reduceGroup(new GroupDegreeTreesToAggregateDegrees(AggregateType.MIN, timePoints));
  }
}

/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.layouting.functions;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

/**
 * Computed the average position of all LVertices in a DataSet
 */
public class AverageVertexPositionsFunction {

  /**
   * Calculates the average positions of the vertices.
   *
   * @param vertices A Dataset of vertices
   * @return A DataSet containing a single Vector-Element representing the average position
   */
  public DataSet<Vector> averagePosition(DataSet<LVertex> vertices) {
    return vertices.map(AverageVertexPositionsFunction::mapToTuple)
      .reduce(AverageVertexPositionsFunction::reduceTuples)
      .map(AverageVertexPositionsFunction::tupleToAverage);
  }

  /**
   * Reduce two tuples by summing them.
   *
   * @param tuple1 First tuple to sum
   * @param tuple2 Second tuple to sum
   * @return Sum of tuple1 and tuple2
   */
  private static Tuple2<Vector, Long> reduceTuples(Tuple2<Vector, Long> tuple1,
    Tuple2<Vector, Long> tuple2) {
    tuple1.f0.mAdd(tuple2.f0);
    tuple1.f1 += tuple2.f1;
    return tuple1;
  }

  /**
   * Create reducable tuples from the vertex-positions.
   *
   * @param vertex Input vertex
   * @return A Tuple containing the vertex position and a long of value 1
   */
  private static Tuple2<Vector, Long> mapToTuple(LVertex vertex) {
    return new Tuple2<>(vertex.getPosition(), 1L);
  }

  /**
   * Convert final reduced tuple to Lvertex
   *
   * @param tuple The result of the reduce
   * @return A single LVertex
   */
  private static Vector tupleToAverage(Tuple2<Vector, Long> tuple) {
    return tuple.f0.mDiv(tuple.f1);
  }
}

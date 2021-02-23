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
package org.gradoop.flink.io.impl.image.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.flink.model.impl.operators.layouting.LayoutingAlgorithm;

/**
 * Function to prepare tuple with coordinates of each vertex.
 */
public class ToCoordsTuple implements MapFunction<EPGMVertex, Tuple4<Integer, Integer, Integer, Integer>> {

  /**
   * Reuse Tuple
   */
  private Tuple4<Integer, Integer, Integer, Integer> coordTuple;

  /**
   * Default Constructor
   */
  public ToCoordsTuple() {
    this.coordTuple = new Tuple4<>();
  }

  @Override
  public Tuple4<Integer, Integer, Integer, Integer> map(EPGMVertex vertex) throws Exception {
    int x = vertex.getPropertyValue(LayoutingAlgorithm.X_COORDINATE_PROPERTY).getInt();
    int y = vertex.getPropertyValue(LayoutingAlgorithm.Y_COORDINATE_PROPERTY).getInt();

    coordTuple.f0 = x;
    coordTuple.f1 = y;
    coordTuple.f2 = x;
    coordTuple.f3 = y;

    return coordTuple;
  }
}

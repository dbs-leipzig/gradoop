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
package org.gradoop.flink.algorithms.gelly.randomjump.functions.gellyvci;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.List;

/**
 * The vertex value used for the Gelly vertex centric iteration.
 */
public class VCIVertexValue extends Tuple2<Boolean, List<GradoopId>> {

  /**
   * Creates an empty instance of VCIVertexValue
   */
  public VCIVertexValue() { }

  /**
   * Creates an instance of VCIVertexValue with the given values.
   *
   * @param visited {@code Boolean} determining if a vertex was visited ({@code true})
   *                or not ({@code false})
   * @param visitedNeighbors {@code List} containing the ids of all visited neighbors
   */
  public VCIVertexValue(Boolean visited, List<GradoopId> visitedNeighbors) {
    super(visited, visitedNeighbors);
  }

  /**
   * Checks if the vertex was visited
   *
   * @return {@code true} if it was visited, {@code false} otherwise
   */
  public boolean isVisited() {
    return this.f0;
  }

  /**
   * Sets the vertex as visited
   */
  public void setVisited() {
    this.f0 = true;
  }

  /**
   * Gets all visited neighbors
   *
   * @return List containing the ids off all visited neighbors
   */
  public List<GradoopId> getVisitedNeighbors() {
    return this.f1;
  }

  /**
   * Adds an id to the list of visited neighbors.
   *
   * @param neighborId The newly visited neighbor id
   */
  public void addVisitedNeighbor(GradoopId neighborId) {
    this.f1.add(neighborId);
  }
}

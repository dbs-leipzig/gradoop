/**
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

package org.gradoop.flink.algorithms.jaccardindex.functions;

import org.apache.flink.api.java.functions.KeySelector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType;

import static org.gradoop.flink.algorithms.jaccardindex.JaccardIndex.NeighborhoodType.OUT;

/**
 * Selects the appropriate Key- or SourceId from an edge depending on the given
 * {@link NeighborhoodType}
 */
public class NeighborhoodTypeEdgeKeySelector implements KeySelector<Edge, GradoopId> {

  /**
   * The type of neighborhood
   */
  private final NeighborhoodType neighborhoodType;

  /**
   * Creates a new EdgeKeySelector with the given neighborhood type
   * @param neighborhoodType type of neighborhood
   */
  public NeighborhoodTypeEdgeKeySelector(NeighborhoodType neighborhoodType) {
    this.neighborhoodType = neighborhoodType;
  }

  @Override
  public GradoopId getKey(Edge value) {
    return neighborhoodType.equals(OUT) ? value.getSourceId() : value.getTargetId();
  }
}

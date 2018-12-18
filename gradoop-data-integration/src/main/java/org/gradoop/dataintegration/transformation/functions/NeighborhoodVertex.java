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
package org.gradoop.dataintegration.transformation.functions;

import org.gradoop.common.model.impl.id.GradoopId;

/**
 * A simple neighbor vertex pojo which contains information about the Id, the connecting edge id
 * and label.
 */
public class NeighborhoodVertex {
  /**
   * The {@link GradoopId} of the neighbor vertex.
   */
  private final GradoopId neighborId;

  /**
   * The {@link GradoopId} of the edge which connects the original vertex and the neighbor.
   */
  private final GradoopId connectingEdgeId;

  /**
   * The edge label of the edge which connects the original vertex and the neighbor.
   */
  private final String connectingEdgeLabel;

  /**
   * A constructor for the Pojo that contains information of a neighbor vertex.
   *
   * @param neighborId The {@link GradoopId} of the neighbor vertex.
   * @param connectingEdgeId The {@link GradoopId} of the edge which connects the original
   *                         vertex and the neighbor.
   * @param connectingEdgeLabel The edge label of the edge which connects the original vertex and
   *                            the neighbor.
   */
  public NeighborhoodVertex(GradoopId neighborId, GradoopId connectingEdgeId,
                    String connectingEdgeLabel) {
    this.neighborId = neighborId;
    this.connectingEdgeId = connectingEdgeId;
    this.connectingEdgeLabel = connectingEdgeLabel;
  }

  /**
   * Get the {@link GradoopId} of the neighbor vertex.
   *
   * @return GradoopId of the Neighbor.
   */
  public GradoopId getNeighborId() {
    return neighborId;
  }

  /**
   * Get the {@link GradoopId} of the edge which connects the original vertex and the neighbor.
   *
   * @return GradoopId of the connecting edge.
   */
  public GradoopId getConnectingEdgeId() {
    return connectingEdgeId;
  }

  /**
   * Get the edge label of the edge which connects the original vertex and the neighbor.
   *
   * @return The edge label of the connecting edge.
   */
  public String getConnectingEdgeLabel() {
    return connectingEdgeLabel;
  }
}

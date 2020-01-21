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

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.model.impl.operators.layouting.CentroidFRLayouter;
import org.gradoop.flink.model.impl.operators.layouting.util.Centroid;
import org.gradoop.flink.model.impl.operators.layouting.util.Force;
import org.gradoop.flink.model.impl.operators.layouting.util.LVertex;
import org.gradoop.flink.model.impl.operators.layouting.util.Vector;

import java.util.List;

/**
 * Calculate repulsion-forces for vertices using centroids
 */
public class CentroidRepulsionForceMapper extends RichMapFunction<LVertex, Force> {
  /**
   * The function to use for the calculation of the repulsion-force
   */
  protected FRRepulsionFunction rf;
  /**
   * Current centroids
   */
  protected List<Centroid> centroids;
  /**
   * Current center of the graph
   */
  protected List<Vector> center;
  /**
   * For object-reuse
   */
  private LVertex centroidVertex = new LVertex();
  /**
   * For object-reuse
   */
  private Vector forceSumVect = new Vector();
  /**
   * For object-reuse
   */
  private Force sumForce = new Force();

  /**
   * Create new calculator
   *
   * @param rf Repulsion function to use
   */
  public CentroidRepulsionForceMapper(FRRepulsionFunction rf) {
    this.rf = rf;
  }


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    centroids = getRuntimeContext().getBroadcastVariable(CentroidFRLayouter.CENTROID_BROADCAST_NAME);
    center = getRuntimeContext().getBroadcastVariable(CentroidFRLayouter.CENTER_BROADCAST_NAME);
  }


  @Override
  public Force map(LVertex vertex) {
    forceSumVect.reset();
    for (Centroid c : centroids) {
      centroidVertex.setId(c.getId());
      centroidVertex.setPosition(c.getPosition().copy());
      forceSumVect.mAdd(rf.join(vertex, centroidVertex).getValue());
    }
    centroidVertex.setPosition(center.get(0));
    forceSumVect.mAdd(rf.join(vertex, centroidVertex).getValue());
    sumForce.set(vertex.getId(), forceSumVect);
    return sumForce;
  }
}

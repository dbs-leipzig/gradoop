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
package org.gradoop.flink.model.impl.operators.difference;

import org.apache.flink.api.java.DataSet;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.flink.model.impl.functions.graphcontainment
  .GraphsContainmentFilterBroadcast;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.model.impl.functions.epgm.Id;
import org.gradoop.flink.model.impl.functions.graphcontainment.InAnyGraphBroadcast;
import org.gradoop.common.model.impl.id.GradoopId;

/**
 * Returns a collection with all logical graphs that are contained in the
 * first input collection but not in the second.
 * Graph equality is based on their respective identifiers.
 * <p>
 * This operator implementation requires that a list of subgraph identifiers
 * in the resulting graph collections fits into the workers main memory.
 */
public class DifferenceBroadcast extends Difference {

  /**
   * Computes the resulting vertices by collecting a list of resulting
   * subgraphs and checking if the vertex is contained in that list.
   *
   * @param newGraphHeads graph dataset of the resulting graph collection
   * @return vertex set of the resulting graph collection
   */
  @Override
  protected DataSet<Vertex> computeNewVertices(
    DataSet<GraphHead> newGraphHeads) {

    DataSet<GradoopId> identifiers = newGraphHeads
      .map(new Id<GraphHead>());

    return firstCollection.getVertices()
      .filter(new InAnyGraphBroadcast<Vertex>())
      .withBroadcastSet(identifiers,
        GraphsContainmentFilterBroadcast.GRAPH_IDS);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public String getName() {
    return DifferenceBroadcast.class.getName();
  }
}

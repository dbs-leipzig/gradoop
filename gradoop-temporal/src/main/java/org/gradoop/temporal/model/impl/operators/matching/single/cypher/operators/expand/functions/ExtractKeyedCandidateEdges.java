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
package org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.operators.expand.pojos.TemporalEdgeWithTiePoint;
import org.gradoop.temporal.model.impl.operators.matching.single.cypher.pojos.EmbeddingTPGM;

/**
 * Extracts join key and time data from an edge embedding and stores them in a
 * {@link TemporalEdgeWithTiePoint}
 */
public class ExtractKeyedCandidateEdges
  extends RichMapFunction<EmbeddingTPGM, TemporalEdgeWithTiePoint> {

  /**
   * Reuse Tuple
   */
  private TemporalEdgeWithTiePoint reuseEdgeWitTiePoint;

  @Override
  public void open(Configuration parameters) throws Exception {
    this.reuseEdgeWitTiePoint = new TemporalEdgeWithTiePoint();
  }

  @Override
  public TemporalEdgeWithTiePoint map(EmbeddingTPGM edge) throws Exception {
    reuseEdgeWitTiePoint.setSource(edge.getId(0));
    reuseEdgeWitTiePoint.setSourceTimeData(edge.getTimes(0));
    reuseEdgeWitTiePoint.setEdge(edge.getId(1));
    reuseEdgeWitTiePoint.setEdgeTimeData(edge.getTimes(1));
    if (edge.size() == 3) {
      // normal edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(2));
      reuseEdgeWitTiePoint.setTargetTimeData(edge.getTimes(2));
    } else {
      // loop edge
      reuseEdgeWitTiePoint.setTarget(edge.getId(0));
      reuseEdgeWitTiePoint.setTargetTimeData(edge.getTimes(0));
    }

    return reuseEdgeWitTiePoint;
  }

}


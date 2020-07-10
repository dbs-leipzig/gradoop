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
package org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning;

import org.gradoop.common.model.impl.pojo.EPGMElement;
import org.gradoop.temporal.model.impl.TemporalGraph;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.TemporalGraphStatisticsFactory;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.functions.ElementsToStats;
import org.gradoop.temporal.model.impl.operators.matching.common.statistics.binning.pojo.TemporalElementStats;
import org.gradoop.temporal.model.impl.pojo.TemporalEdge;
import org.gradoop.temporal.model.impl.pojo.TemporalVertex;

import java.util.List;

public class BinningTemporalGraphStatisticsFactory implements
  TemporalGraphStatisticsFactory<BinningTemporalGraphStatistics> {
  @Override
  public BinningTemporalGraphStatistics fromGraph(TemporalGraph g) {
    return fromGraphWithSampling(g, 5000);
  }

  @Override
  public BinningTemporalGraphStatistics fromGraphWithSampling(TemporalGraph g, int sampleSize) {
    try {
      List<TemporalElementStats> vertexStats = g.getVertices()
        .groupBy(EPGMElement::getLabel)
        .reduceGroup(new ElementsToStats<TemporalVertex>())
        .collect();
      List<TemporalElementStats> edgeStats = g.getEdges()
        // do not replace this with the method reference!!!
        .groupBy(edge -> edge.getLabel())
        .reduceGroup(new ElementsToStats<TemporalEdge>())
        .collect();
      return new BinningTemporalGraphStatistics(vertexStats, edgeStats);

    } catch(Exception e) {
      System.out.println(e.getMessage());
    }
    return null;
  }
}

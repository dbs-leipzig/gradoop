/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithoutVertexLabels;
import org.gradoop.common.model.api.entities.EPGMEdge;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.common.model.impl.id.GradoopId;

import java.util.Map;

/**
 * (graphId, sourceIdId, targetValue, stringLabel)
 * |><| (stringLabel, integerLabel)
 * => (graphId, sourceIdId, targetValue, integerLabel)
 * @param <E> EPGM edge type
 */

public class EdgeLabelEncoder<E extends EPGMEdge>
  extends RichFlatMapFunction<E, EdgeTripleWithoutVertexLabels> {

  /**
   * edge label dictionary
   */
  private Map<String, Integer> dictionary;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.dictionary = getRuntimeContext().<Map<String, Integer>>
      getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY).get(0);
  }

  @Override
  public void flatMap(
    E edge, Collector<EdgeTripleWithoutVertexLabels> collector) throws
    Exception {

    Integer intLabel = dictionary.get(edge.getLabel());

    if (intLabel != null) {
      for (GradoopId graphId : edge.getGraphIds()) {
        collector.collect(new EdgeTripleWithoutVertexLabels(
          graphId, edge.getSourceId(), edge.getTargetId(), intLabel));
      }
    }
  }
}

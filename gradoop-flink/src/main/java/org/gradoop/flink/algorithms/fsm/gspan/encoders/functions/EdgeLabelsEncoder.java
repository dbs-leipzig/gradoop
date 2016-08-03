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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithoutGraphId;

import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTriple;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithStringEdgeLabel;

import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;

import java.util.Collection;
import java.util.Map;

/**
 * [e0,..,eN] => G
 * with edges and graph in gSpan specific representation;
 * edge labels are translated from string to integer
 *
 * @param <IDT> Id type
 */
public class EdgeLabelsEncoder<IDT> extends RichMapFunction
  <Collection<EdgeTripleWithStringEdgeLabel<IDT>>, GSpanGraph> {

  /**
   * edge label dictionary
   */
  private Map<String, Integer> dictionary;
  /**
   * FSM configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public EdgeLabelsEncoder(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.dictionary = getRuntimeContext().<Map<String, Integer>>
      getBroadcastVariable(BroadcastNames.EDGE_DICTIONARY).get(0);
  }

  @Override
  public GSpanGraph map(
    Collection<EdgeTripleWithStringEdgeLabel<IDT>> stringTriples)
      throws Exception {

    Collection<EdgeTriple<IDT>> intTriples = Lists.newArrayList();

    for (EdgeTripleWithStringEdgeLabel<IDT> triple : stringTriples) {
      Integer edgeLabel = dictionary.get(triple.getEdgeLabel());

      if (edgeLabel != null) {
        intTriples.add(new EdgeTripleWithoutGraphId<IDT>(
          triple.getSourceId(),
          triple.getTargetId(),
          edgeLabel,
          triple.getSourceLabel(),
          triple.getTargetLabel()
        ));
      }
    }

    return GSpan.createGSpanGraph(intTriples, fsmConfig);
  }
}

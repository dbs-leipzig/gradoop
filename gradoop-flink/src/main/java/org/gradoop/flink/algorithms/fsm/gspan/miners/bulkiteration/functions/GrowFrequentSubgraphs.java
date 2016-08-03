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

package org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.config.BroadcastNames;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.miners.bulkiteration.pojos.IterationItem;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;

/**
 * Core of gSpan implementation. Grows embeddings of CompleteResult DFS codes.
 */
public class GrowFrequentSubgraphs
  extends RichMapFunction<IterationItem, IterationItem> {

  /**
   * frequent subgraph mining configuration
   */
  private final FSMConfig fsmConfig;
  /**
   * frequent DFS codes
   */
  private Collection<DFSCode> frequentSubgraphs;

  /**
   * constructor
   * @param fsmConfig configuration
   */
  public GrowFrequentSubgraphs(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<WithCount<CompressedDFSCode>> frequentSubgraphsWithFrequency =
      getRuntimeContext()
        .getBroadcastVariable(BroadcastNames.FREQUENT_SUBGRAPHS);

    frequentSubgraphs = Lists.newArrayList();

    for (WithCount<CompressedDFSCode> frequentSubgraph :
      frequentSubgraphsWithFrequency) {

      frequentSubgraphs.add(frequentSubgraph.getObject().getDfsCode());
    }
  }

  @Override
  public IterationItem map(IterationItem iterationItem) throws Exception {

    if (! iterationItem.isCollector()) {
      GSpan.growEmbeddings(
        iterationItem.getTransaction(), frequentSubgraphs, fsmConfig);
    }

    return iterationItem;
  }
}

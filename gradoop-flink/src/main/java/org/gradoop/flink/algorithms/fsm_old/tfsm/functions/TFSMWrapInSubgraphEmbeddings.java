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

package org.gradoop.flink.algorithms.fsm_old.tfsm.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm_old.tfsm.tuples.TFSMSubgraph;
import org.gradoop.flink.algorithms.fsm_old.tfsm.tuples.TFSMSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class TFSMWrapInSubgraphEmbeddings implements
  MapFunction<TFSMSubgraph, TFSMSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final TFSMSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public TFSMWrapInSubgraphEmbeddings() {
    this.reuseTuple = new TFSMSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
    this.reuseTuple.setSize(0);
  }

  @Override
  public TFSMSubgraphEmbeddings map(TFSMSubgraph tfsmSubgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(tfsmSubgraph.getCanonicalLabel());
    reuseTuple.setEmbeddings(Lists.newArrayList(tfsmSubgraph.getEmbedding()));

    return reuseTuple;
  }
}

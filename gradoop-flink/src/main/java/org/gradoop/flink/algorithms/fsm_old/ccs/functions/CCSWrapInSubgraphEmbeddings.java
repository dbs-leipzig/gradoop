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

package org.gradoop.flink.algorithms.fsm_old.ccs.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.algorithms.fsm_old.ccs.tuples.CCSSubgraph;
import org.gradoop.flink.algorithms.fsm_old.ccs.tuples.CCSSubgraphEmbeddings;

/**
 * subgraphWithSampleEmbedding => subgraphWithEmbeddings
 */
public class CCSWrapInSubgraphEmbeddings implements
  MapFunction<CCSSubgraph, CCSSubgraphEmbeddings> {

  /**
   * reuse tuple to avoid instantiations
   */
  private final CCSSubgraphEmbeddings reuseTuple;

  /**
   * Constructor.
   */
  public CCSWrapInSubgraphEmbeddings() {
    this.reuseTuple = new CCSSubgraphEmbeddings();
    this.reuseTuple.setGraphId(GradoopId.NULL_VALUE);
  }

  @Override
  public CCSSubgraphEmbeddings map(CCSSubgraph subgraph) throws
    Exception {

    reuseTuple.setCanonicalLabel(subgraph.getCanonicalLabel());
    reuseTuple.setCategory(subgraph.getCategory());
    reuseTuple.setSize(subgraph.getEmbedding().getEdges().size());
    reuseTuple.setEmbeddings(Lists.newArrayList(subgraph.getEmbedding()));

    return reuseTuple;
  }
}

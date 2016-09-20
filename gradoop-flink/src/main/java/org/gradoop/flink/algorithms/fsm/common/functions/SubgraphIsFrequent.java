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

package org.gradoop.flink.algorithms.fsm.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.common.config.Constants;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Collection;

/**
 * Evaluates to true if the subgraph of an embeddings list is frequent.
 *
 * @param <S> subgraph embeddings type
 */
public class SubgraphIsFrequent<S extends SubgraphEmbeddings>
  extends RichFilterFunction<S> {

  /**
   * frequent subgraphs received via broadcast
   */
  private Collection<String> frequentSubgraphs;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.frequentSubgraphs = getRuntimeContext()
      .getBroadcastVariable(Constants.FREQUENT_SUBGRAPHS);

    this.frequentSubgraphs = Sets.newHashSet(frequentSubgraphs);
  }
  @Override
  public boolean filter(S value) throws Exception {
    return frequentSubgraphs.contains(value.getCanonicalLabel());
  }
}

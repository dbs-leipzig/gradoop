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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.common.pojos.Union;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Joins 1-edge embeddings to 2-edge embeddings.
 *
 * @param <SE> subgraph type
 */
public class JoinSingleEdgeEmbeddings<SE extends SubgraphEmbeddings>
  extends JoinEmbeddings<SE> implements GroupReduceFunction<SE, SE> {

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  public JoinSingleEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void reduce(Iterable<SE> values, Collector<SE> out) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();
    Set<Union> unions = Sets.newHashSet();

    Map<String, Collection<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    SE reuseTuple = null;
    for (SE subgraphEmbedding : values) {
      if (reuseTuple == null) {
        reuseTuple = subgraphEmbedding;
        reuseTuple.setSize(2);
      }

      for (Embedding left : subgraphEmbedding.getEmbeddings()) {
        for (Embedding right : cachedEmbeddings) {

          if (left.sharesVerticesWith(right)) {

            Set<Integer> leftEdgeIds = left.getEdgeIds();
            Set<Integer> rightEdgeIds = right.getEdgeIds();
            Union union = new Union(leftEdgeIds, rightEdgeIds);

            if (union.size() == 2) {

              if (! unions.contains(union)) {
                unions.add(union);
                Embedding embedding = left.combine(right);

                String subgraph = canonicalLabeler.label(embedding);

                Collection<Embedding> embeddings = subgraphEmbeddings
                  .get(subgraph);

                if (embeddings == null) {
                  subgraphEmbeddings
                    .put(subgraph, Lists.newArrayList(embedding));
                } else {
                  embeddings.add(embedding);
                }
              }
            }
          }
        }
        cachedEmbeddings.add(left);
      }
    }

    collect(reuseTuple, out, subgraphEmbeddings);
  }
}

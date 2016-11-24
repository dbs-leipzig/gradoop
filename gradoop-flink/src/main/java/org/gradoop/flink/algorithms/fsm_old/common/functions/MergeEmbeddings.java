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

package org.gradoop.flink.algorithms.fsm_old.common.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm_old.common.tuples.SubgraphEmbeddings;

import java.util.Iterator;

/**
 * (graph, subgraph, embeddings),.. => (graph, "", embeddings)
 *
 * @param <SE>
 */
public class MergeEmbeddings<SE extends SubgraphEmbeddings>
  implements GroupReduceFunction<SE, SE> {

  @Override
  public void reduce(Iterable<SE> iterable,
    Collector<SE> collector) throws Exception {

    Iterator<SE> iterator = iterable.iterator();

    SE out = iterator.next();
    out.setCanonicalLabel("");

    while (iterator.hasNext()) {
      out.getEmbeddings().addAll(iterator.next().getEmbeddings());
    }

    collector.collect(out);
  }
}

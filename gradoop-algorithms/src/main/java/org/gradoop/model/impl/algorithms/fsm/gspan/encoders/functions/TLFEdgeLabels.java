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

package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples.EdgeTripleWithStringEdgeLabel;


import org.gradoop.model.impl.algorithms.fsm.gspan.encoders.tuples
  .TLFEdgeTriple;

import java.util.Collection;
import java.util.Set;

/**
 * {e0,..,eN} => le0,..,leM
 *
 * flatmaps an edge collection to distinct edge labels
 */
public class TLFEdgeLabels implements
  FlatMapFunction<Collection<TLFEdgeTriple>, String> {

  @Override
  public void flatMap(
    Collection<TLFEdgeTriple> edges, Collector<String> collector)
    throws Exception {

    for (TLFEdgeTriple edge : edges) {
      collector.collect(edge.getEdgeLabel());
    }
  }
}

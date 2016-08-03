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

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.SerializedDFSCode;
import org.gradoop.flink.model.impl.tuples.WithCount;

/**
 * G => compress(G) IF G is canonical
 */
public class PostPruneAndCompress implements FlatMapFunction
  <WithCount<SerializedDFSCode>, WithCount<CompressedDFSCode>> {

  /**
   * frequent subgraph mining configuration
   */
  private final FSMConfig fsmConfig;

  /**
   * constructor
   *
   * @param fsmConfig frequent subgraph mining configuration
   */
  public PostPruneAndCompress(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void flatMap(WithCount<SerializedDFSCode> subgraph,
    Collector<WithCount<CompressedDFSCode>> collector) throws Exception {

    DFSCode code = subgraph.getObject().getDfsCode();
    int support = subgraph.getCount();

    if (GSpan.isMinimal(code, fsmConfig)) {
      collector.collect(new WithCount<>(new CompressedDFSCode(code), support));
    }
  }
}

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

package org.gradoop.flink.algorithms.fsm.transactional.gspan.functions;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.gspan.algorithm.GSpanKernelBase;
import org.gradoop.flink.model.impl.tuples.WithCount;
import org.gradoop.flink.representation.transactional.GraphTransaction;
import org.gradoop.flink.representation.transactional.traversalcode.TraversalCode;

/**
 * DFS-code => (g, V, E)
 */
public class ToGraphTransaction
  extends RichMapFunction<WithCount<TraversalCode<String>>, GraphTransaction> {

  /**
   * graph count
   */
  private long graphCount;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    this.graphCount = getRuntimeContext()
      .<Long>getBroadcastVariable(TFSMConstants.GRAPH_COUNT).get(0);
  }

  @Override
  public GraphTransaction map(WithCount<TraversalCode<String>> patternWithCount) throws Exception {
    return GSpanKernelBase.createGraphTransaction(patternWithCount, graphCount);
  }
}

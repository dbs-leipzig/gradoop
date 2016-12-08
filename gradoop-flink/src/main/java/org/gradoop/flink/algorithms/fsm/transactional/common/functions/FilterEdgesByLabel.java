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

package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Drops edges with infrequent labels and isolated vertices.
 */
public class FilterEdgesByLabel extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * frequent edge labels
   */
  private Collection<String> frequentEdgeLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<String> broadcast = getRuntimeContext()
      .getBroadcastVariable(TFSMConstants.FREQUENT_EDGE_LABELS);

    this.frequentEdgeLabels = Sets.newHashSet(broadcast);
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    Set<GradoopId> referenceEdgeIds = Sets.newHashSet();

    // drop edges with infrequent labels

    Iterator<Edge> edgeIterator = transaction.getEdges().iterator();

    while (edgeIterator.hasNext()) {
      Edge next = edgeIterator.next();

      if (frequentEdgeLabels.contains(next.getLabel())) {
        referenceEdgeIds.add(next.getSourceId());
        referenceEdgeIds.add(next.getTargetId());
      } else {
        edgeIterator.remove();
      }
    }

    // drop vertex without any edges

    transaction.getVertices().removeIf(next -> !referenceEdgeIds.contains(next.getId()));

    return transaction;
  }
}

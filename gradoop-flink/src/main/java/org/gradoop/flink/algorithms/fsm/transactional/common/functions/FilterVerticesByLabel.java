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
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.representation.transactional.GraphTransaction;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

/**
 * Drops vertices with infrequent labels and their incident edges.
 */
public class FilterVerticesByLabel extends RichMapFunction<GraphTransaction, GraphTransaction> {

  /**
   * frequent vertex labels
   */
  private Collection<String> frequentVertexLabels;

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);

    Collection<String> broadcast = getRuntimeContext()
      .getBroadcastVariable(TFSMConstants.FREQUENT_VERTEX_LABELS);

    this.frequentVertexLabels = Sets.newHashSet(broadcast);
  }

  @Override
  public GraphTransaction map(GraphTransaction transaction) throws Exception {

    Set<GradoopId> keptVertexIds = Sets.newHashSet();

    // drop vertices with infrequent labels

    Iterator<Vertex> vertexIterator = transaction.getVertices().iterator();

    while (vertexIterator.hasNext()) {
      Vertex next = vertexIterator.next();

      if (frequentVertexLabels.contains(next.getLabel())) {
        keptVertexIds.add(next.getId());
      } else {
        vertexIterator.remove();
      }
    }

    // drop inconsistent edges

    transaction.getEdges().removeIf(next ->
      !keptVertexIds.contains(next.getSourceId()) || !keptVertexIds.contains(next.getTargetId()));

    return transaction;
  }
}

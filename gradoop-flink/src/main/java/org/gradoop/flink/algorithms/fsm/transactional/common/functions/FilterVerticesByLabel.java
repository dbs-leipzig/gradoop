/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.fsm.transactional.common.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;

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

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

package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.gradoop.flink.algorithms.fsm.transactional.tle.canonicalization.api.CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * Superclass of flatmap functions extracting distinct single-edge subgraphs
 * and all embeddings from a graph.
 */
public abstract class SingleEdgeEmbeddings implements Serializable {

  /**
   * graph labeler
   */
  protected CanonicalLabeler canonicalLabeler;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration
   */
  public SingleEdgeEmbeddings(FSMConfig fsmConfig) {
    canonicalLabeler = fsmConfig.getCanonicalLabeler();
  }

  /**
   * Finds all embeddings.
   *
   * @param graph graph
   * @return 1-edge embeddings
   */
  protected Map<String, List<Embedding>> createEmbeddings(
    FSMGraph graph) {

    Map<Integer, String> vertices = graph.getVertices();
    Map<String, List<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    for (Map.Entry<Integer, FSMEdge> entry : graph.getEdges().entrySet()) {

      FSMEdge edge = entry.getValue();
      int sourceId = edge.getSourceId();
      int targetId = edge.getTargetId();

      Map<Integer, String> incidentVertices =
        Maps.newHashMapWithExpectedSize(2);

      incidentVertices.put(sourceId, vertices.get(sourceId));

      if (sourceId != targetId) {
        incidentVertices.put(targetId, vertices.get(targetId));
      }

      Map<Integer, FSMEdge> singleEdge = Maps.newHashMapWithExpectedSize(1);
      singleEdge.put(entry.getKey(), edge);

      Embedding
        embedding = new Embedding(incidentVertices, singleEdge);

      String subgraph = canonicalLabeler.label(embedding);

      List<Embedding> embeddings = subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings.put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }

    return subgraphEmbeddings;
  }
}

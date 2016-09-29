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
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.common.canonicalization.api.CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm.common.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.common.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMGraph;
import org.gradoop.flink.algorithms.fsm.common.pojos.Union;
import org.gradoop.flink.algorithms.fsm.common.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Superclass of classes joining subgraph embeddings.
 *
 * @param <SE> subgraph type
 */
public class PatternGrowth<G extends FSMGraph, SE extends SubgraphEmbeddings>
  implements FlatJoinFunction<SE, G, SE> {

  /**
   * Labeler used to generate canonical labels.
   */
  private final CanonicalLabeler canonicalLabeler;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  public PatternGrowth(FSMConfig fsmConfig) {
    this.canonicalLabeler = fsmConfig.getCanonicalLabeler();
  }

  @Override
  public void join(SE embeddings,
    G graph, Collector<SE> out) throws
    Exception {

    Set<Union> unions = Sets.newHashSet();
    Map<String, Collection<Embedding>> subgraphEmbeddings = Maps.newHashMap();

    for (Embedding parent : embeddings.getEmbeddings()) {
      for (Map.Entry<Integer, FSMEdge> entry : graph.getEdges().entrySet()) {

        int edgeId = entry.getKey();

        if (! parent.getEdges().containsKey(edgeId)) {
          FSMEdge edge = entry.getValue();

          int sourceId = edge.getSourceId();
          boolean containsSourceId =
            parent.getVertices().containsKey(sourceId);

          int targetId = edge.getTargetId();
          boolean containsTargetId =
            parent.getVertices().containsKey(targetId);

          if (containsSourceId ||  containsTargetId) {
            Union union = new Union(parent.getEdgeIds(), edgeId);

            if (! unions.contains(union)) {
              unions.add(union);

              Embedding child = parent.deepCopy();
              child.getEdges().put(edgeId, edge);

              if (! containsSourceId) {
                child.getVertices()
                  .put(sourceId, graph.getVertices().get(sourceId));
              }
              if (! containsTargetId) {
                child.getVertices()
                  .put(targetId, graph.getVertices().get(targetId));
              }

              String canonicalLabel = canonicalLabeler.label(child);

              Collection<Embedding> siblings =
                subgraphEmbeddings.get(canonicalLabel);

              if (siblings == null) {
                siblings = Lists.newArrayList(child);
                subgraphEmbeddings.put(canonicalLabel, siblings);
              } else {
                siblings.add(child);
              }
            }
          }
        }
      }
    }

    embeddings.setSize(embeddings.getSize() + 1);
    collect(embeddings, out, subgraphEmbeddings);
  }

  /**
   * Triggers output of subgraphs and embeddings.
   *
   * @param reuseTuple reuse tuple to avoid instantiations
   * @param out Flink output collector
   * @param subgraphEmbeddings subgraphs and embeddings.
   */
  private void collect(SE reuseTuple, Collector<SE> out,
    Map<String, Collection<Embedding>> subgraphEmbeddings) {

    for (Map.Entry<String, Collection<Embedding>> entry  :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }
}

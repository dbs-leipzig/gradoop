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

package org.gradoop.flink.algorithms.fsm.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.pojos.Intersection;
import org.gradoop.flink.algorithms.fsm.tuples.SubgraphEmbeddings;


import org.gradoop.flink.algorithms.fsm.pojos.Embedding;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Joins k-edge embeddings to 2k-edge and 2k-1-edge embeddings.
 */
public class JoinMultiEdgeEmbeddings extends JoinEmbeddings {

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  public JoinMultiEdgeEmbeddings(FSMConfig fsmConfig) {
    super(fsmConfig);
  }

  @Override
  public void reduce(Iterable<SubgraphEmbeddings> values,
    Collector<SubgraphEmbeddings> out) throws Exception {

    Collection<Embedding> cachedEmbeddings = Lists.newArrayList();

    Set<Intersection> oddIntersections = Sets.newHashSet();
    Map<String, Collection<Embedding>> oddSubgraphEmbeddings =
      Maps.newHashMap();

    Set<Intersection> evenIntersections = Sets.newHashSet();
    Map<String, Collection<Embedding>> evenSubgraphEmbeddings =
      Maps.newHashMap();

    boolean first = true;

    for (SubgraphEmbeddings subgraphEmbedding : values) {
      if (first) {
        reuseTuple.setGraphId(subgraphEmbedding.getGraphId());
        reuseTuple.setSize(subgraphEmbedding.getSize() * 2 - 1);
        first = false;
      }

      for (Embedding left : subgraphEmbedding.getEmbeddings()) {
        for (Embedding right : cachedEmbeddings) {

          if (left.sharesVerticesWith(right)) {

            Set<Integer> leftEdgeIds = left.getEdgeIds();
            Set<Integer> rightEdgeIds = right.getEdgeIds();
            Intersection
              intersection = new Intersection(leftEdgeIds, rightEdgeIds);

            int overlappingEdgeCount =
              leftEdgeIds.size() + rightEdgeIds.size() - intersection.size();

            if (overlappingEdgeCount == 0) {
              addEmbedding(
                evenSubgraphEmbeddings, left, right, evenIntersections,
                intersection);

            } else if (overlappingEdgeCount == 1) {
              addEmbedding(
                oddSubgraphEmbeddings, left, right, oddIntersections,
                intersection);
            }
          }
        }
        cachedEmbeddings.add(left);
      }
    }

    collect(out, oddSubgraphEmbeddings);

    reuseTuple.setSize(reuseTuple.getSize() + 1);

    collect(out, evenSubgraphEmbeddings);
  }

  /**
   * Creates an embedding by joining two others and stores it for output, if
   * not already discovered before.
   *
   * @param subgraphEmbeddings output storage
   * @param left first embedding
   * @param right second embedding
   * @param intersections discovered embeddings' intersections
   * @param intersection current embedding's intersection
   */
  private void addEmbedding(
    Map<String, Collection<Embedding>> subgraphEmbeddings,
    Embedding left, Embedding right,
    Set<Intersection> intersections, Intersection intersection) {

    if (!intersections.contains(intersection)) {

      intersections.add(intersection);
      Embedding embedding = left.combine(right);

      String subgraph = canonicalLabeler.label(embedding);

      Collection<Embedding> embeddings =
        subgraphEmbeddings.get(subgraph);

      if (embeddings == null) {
        subgraphEmbeddings
          .put(subgraph, Lists.newArrayList(embedding));
      } else {
        embeddings.add(embedding);
      }
    }
  }
}

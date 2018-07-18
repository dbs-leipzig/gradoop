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
package org.gradoop.flink.algorithms.fsm.transactional.tle.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.algorithms.fsm.transactional.common.FSMConfig;
import org.gradoop.flink.algorithms.fsm.transactional.tle.canonicalization.CanonicalLabeler;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.Embedding;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMGraph;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.SubgraphEmbeddings;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Superclass of classes joining subgraph embeddings.
 *
 * @param <G> graph type
 * @param <SE> subgraph type
 */
public class JoinEmbeddings<G extends FSMGraph, SE extends SubgraphEmbeddings>
  implements FlatJoinFunction<SE, G, SE>, FlatMapFunction<SE, SE> {

  /**
   * Labeler used to generate canonical labels.
   */
  private final CanonicalLabeler canonicalLabeler;

  /**
   * Constructor.
   *
   * @param fsmConfig FSM configuration.
   */
  public JoinEmbeddings(FSMConfig fsmConfig) {
    this.canonicalLabeler = new CanonicalLabeler(fsmConfig.isDirected());
  }

  @Override
  public void join(SE embeddings, G graph, Collector<SE> out) throws
    Exception {

    Set<TreeSet<Integer>> edgeSets = Sets.newHashSet();
    Map<String, List<Embedding>> subgraphEmbeddings = Maps.newHashMap();

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
            TreeSet<Integer> edgeSet = Sets.newTreeSet(parent.getEdgeIds());
            edgeSet.add(edgeId);

            if (! edgeSets.contains(edgeSet)) {
              edgeSets.add(edgeSet);

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

              List<Embedding> siblings =
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

    collect(embeddings, out, subgraphEmbeddings);
  }

  @Override
  public void flatMap(SE embeddings, Collector<SE> collector) throws Exception {

    Map<String, List<Embedding>> subgraphEmbeddings;

    if (embeddings.getSize() == 1) {
      subgraphEmbeddings = joinOnVertex(embeddings);
    } else {
      subgraphEmbeddings = joinOnEdgeOverlap(embeddings);
    }

    collect(embeddings, collector, subgraphEmbeddings);
  }

  /**
   * Cross-joins k-edge (k>1) embeddings on k-1-edge overlap.
   *
   * @param embeddings k-edge embeddings
   * @return Map of k+1-edge subgraphs and embeddings
   */
  private Map<String, List<Embedding>> joinOnEdgeOverlap(SE embeddings) {
    Map<String, List<Embedding>> subgraphEmbeddings;
    subgraphEmbeddings = Maps.newHashMap();
    List<Embedding> parents = embeddings.getEmbeddings();

    Set<TreeSet<Integer>> edgeSets = Sets.newHashSet();

    // index parents

    Map<TreeSet<Integer>, List<Integer>> subsetParents =  Maps.newHashMap();

    int parentIndex = 0;
    for (Embedding parent : parents) {

      for (Integer edgeId : parent.getEdgeIds()) {
        TreeSet<Integer> subset = Sets.newTreeSet(parent.getEdgeIds());
        subset.remove(edgeId);

        Collection<Integer> siblings = subsetParents.get(subset);

        if (siblings == null) {
          subsetParents.put(subset, Lists.newArrayList(parentIndex));
        } else {
          siblings.add(parentIndex);
        }
      }

      parentIndex++;
    }

    // join overlapping parents

    for (List<Integer> overlappingParents : subsetParents.values()) {
      int size = overlappingParents.size();

      if (size > 1) {

        for (int i = 0; i < size - 1; i++) {
          for (int j = i + 1; j < size; j++) {

            Embedding left = parents.get(overlappingParents.get(i));
            Embedding right = parents.get(overlappingParents.get(j));

            TreeSet<Integer> edgeSet = Sets.newTreeSet(left.getEdgeIds());
            edgeSet.addAll(right.getEdgeIds());

            if (!edgeSets.contains(edgeSet)) {
              edgeSets.add(edgeSet);

              Embedding child = left.deepCopy();
              child.getVertices().putAll(right.getVertices());
              child.getEdges().putAll(right.getEdges());

              String canonicalLabel = canonicalLabeler.label(child);

              List<Embedding> siblings =
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
    return subgraphEmbeddings;
  }

  /**
   * Cross-joins 1-edge embeddings on source and target vertices.
   *
   * @param embeddings 1-edge embeddings
   * @return Map of 2-edge subgraphs and embeddings
   */
  private Map<String, List<Embedding>> joinOnVertex(SE embeddings) {
    Map<String, List<Embedding>> subgraphEmbeddings;
    subgraphEmbeddings = Maps.newHashMap();
    List<Embedding> parents = embeddings.getEmbeddings();

    int size = parents.size();

    for (int i = 0; i < size - 1; i++) {
      for (int j = i + 1; j < size; j++) {

        Embedding left = parents.get(i);
        Embedding right = parents.get(j);

        FSMEdge leftEdge = left.getEdges().values().iterator().next();
        FSMEdge rightEdge = right.getEdges().values().iterator().next();

        if (leftEdge.getSourceId() == rightEdge.getSourceId() ||
          leftEdge.getSourceId() == rightEdge.getTargetId() ||
          leftEdge.getTargetId() == rightEdge.getSourceId() ||
          leftEdge.getTargetId() == rightEdge.getTargetId()) {

          Embedding child = left.deepCopy();
          child.getVertices().putAll(right.getVertices());
          child.getEdges().putAll(right.getEdges());

          String canonicalLabel = canonicalLabeler.label(child);

          List<Embedding> siblings =
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
    return subgraphEmbeddings;
  }

  /**
   * Triggers output of subgraphs and embeddings.
   *
   * @param reuseTuple reuse tuple to avoid instantiations
   * @param out Flink output collector
   * @param subgraphEmbeddings subgraphs and embeddings.
   */
  private void collect(SE reuseTuple, Collector<SE> out,
    Map<String, List<Embedding>> subgraphEmbeddings) {

    reuseTuple.setSize(reuseTuple.getSize() + 1);

    for (Map.Entry<String, List<Embedding>> entry :
      subgraphEmbeddings.entrySet()) {

      reuseTuple.setCanonicalLabel(entry.getKey());
      reuseTuple.setEmbeddings(entry.getValue());

      out.collect(reuseTuple);
    }
  }
}

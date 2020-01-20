/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.gradoop.common.model.api.entities.EdgeFactory;
import org.gradoop.common.model.api.entities.GraphHeadFactory;
import org.gradoop.common.model.api.entities.VertexFactory;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.EPGMEdge;
import org.gradoop.common.model.impl.pojo.EPGMGraphHead;
import org.gradoop.common.model.impl.pojo.EPGMVertex;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.algorithms.fsm.transactional.common.TFSMConstants;
import org.gradoop.flink.algorithms.fsm.transactional.tle.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.transactional.tle.tuples.Subgraph;
import org.gradoop.flink.model.impl.layouts.transactional.tuples.GraphTransaction;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Superclass of map functions mapping FSM-fitted subgraph representations to
 * Gradoop graph transactions.
 */
public abstract class SubgraphDecoder implements Serializable {

  /**
   * graph Head Factory
   */
  protected final GraphHeadFactory<EPGMGraphHead> graphHeadFactory;
  /**
   * vertex Factory
   */
  protected final VertexFactory<EPGMVertex> vertexFactory;
  /**
   * edge Factory
   */
  protected final EdgeFactory<EPGMEdge> edgeFactory;

  /**
   * Constructor.
   *
   * @param config Gradoop configuration
   */
  public SubgraphDecoder(GradoopFlinkConfig config) {
    vertexFactory = config.getLogicalGraphFactory().getVertexFactory();
    graphHeadFactory = config.getLogicalGraphFactory().getGraphHeadFactory();
    edgeFactory = config.getLogicalGraphFactory().getEdgeFactory();
  }

  /**
   * Turns the subgraph into a Gradoop graph transaction.
   *
   * @param subgraph subgraph
   * @param canonicalLabel canonical label
   * @return graph transaction
   */
  protected GraphTransaction createTransaction(
    Subgraph subgraph, String canonicalLabel) {

    // GRAPH HEAD

    Properties properties = new Properties();

    properties.set(TFSMConstants.SUPPORT_KEY, subgraph.getCount());
    properties.set(TFSMConstants.CANONICAL_LABEL_KEY, subgraph.getCanonicalLabel());

    EPGMGraphHead epgmGraphHead = graphHeadFactory
      .createGraphHead(canonicalLabel, properties);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(epgmGraphHead.getId());

    // VERTICES

    Map<Integer, String> vertices = subgraph.getEmbedding().getVertices();
    Set<EPGMVertex> epgmVertices = Sets.newHashSetWithExpectedSize(vertices.size());
    Map<Integer, GradoopId> vertexIdMap =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {
      EPGMVertex epgmVertex = vertexFactory
        .createVertex(vertex.getValue(), graphIds);

      vertexIdMap.put(vertex.getKey(), epgmVertex.getId());
      epgmVertices.add(epgmVertex);
    }

    // EDGES

    Collection<FSMEdge> edges = subgraph.getEmbedding().getEdges().values();
    Set<EPGMEdge> epgmEdges = Sets.newHashSetWithExpectedSize(edges.size());

    for (FSMEdge edge : edges) {
      epgmEdges.add(edgeFactory.createEdge(
        edge.getLabel(),
        vertexIdMap.get(edge.getSourceId()),
        vertexIdMap.get(edge.getTargetId()),
        graphIds
      ));
    }

    return new GraphTransaction(epgmGraphHead, epgmVertices, epgmEdges);
  }
}

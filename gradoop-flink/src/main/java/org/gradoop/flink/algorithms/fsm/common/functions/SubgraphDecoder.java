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

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.common.model.impl.properties.PropertyList;
import org.gradoop.flink.algorithms.fsm.common.pojos.FSMEdge;
import org.gradoop.flink.algorithms.fsm.common.tuples.Subgraph;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
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
   * Property key to store a frequent subgraphs's frequency.
   */
  public static final String FREQUENCY_KEY = "frequency";
  /**
   * Property key to store the canonical label.
   */
  public static final String CANONICAL_LABEL_KEY = "canonicalLabel";
  /**
   * graph Head Factory
   */
  protected final GraphHeadFactory graphHeadFactory;
  /**
   * vertex Factory
   */
  protected final VertexFactory vertexFactory;
  /**
   * edge Factory
   */
  protected final EdgeFactory edgeFactory;

  /**
   * Constructor.
   *
   * @param config Gradoop configuration
   */
  public SubgraphDecoder(GradoopFlinkConfig config) {
    vertexFactory = config.getVertexFactory();
    graphHeadFactory = config.getGraphHeadFactory();
    edgeFactory = config.getEdgeFactory();
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

    PropertyList properties = new PropertyList();

    properties.set(FREQUENCY_KEY, subgraph.getCount());
    properties.set(CANONICAL_LABEL_KEY, subgraph.getCanonicalLabel());

    GraphHead epgmGraphHead = graphHeadFactory
      .createGraphHead(canonicalLabel, properties);

    GradoopIdSet graphIds = GradoopIdSet.fromExisting(epgmGraphHead.getId());

    // VERTICES

    Map<Integer, String> vertices = subgraph.getEmbedding().getVertices();
    Set<Vertex> epgmVertices = Sets.newHashSetWithExpectedSize(vertices.size());
    Map<Integer, GradoopId> vertexIdMap =
      Maps.newHashMapWithExpectedSize(vertices.size());

    for (Map.Entry<Integer, String> vertex : vertices.entrySet()) {
      Vertex epgmVertex = vertexFactory
        .createVertex(vertex.getValue(), graphIds);

      vertexIdMap.put(vertex.getKey(), epgmVertex.getId());
      epgmVertices.add(epgmVertex);
    }

    // EDGES

    Collection<FSMEdge> edges = subgraph.getEmbedding().getEdges().values();
    Set<Edge> epgmEdges = Sets.newHashSetWithExpectedSize(edges.size());

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

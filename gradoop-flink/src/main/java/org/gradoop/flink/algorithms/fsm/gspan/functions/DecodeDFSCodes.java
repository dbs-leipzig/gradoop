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

package org.gradoop.flink.algorithms.fsm.gspan.functions;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.id.GradoopIdSet;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.EdgeFactory;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.common.model.impl.pojo.VertexFactory;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSStep;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Decodes compressed DFS codes of a partition to respective graph
 * transactions.
 */
public class DecodeDFSCodes extends
  RichMapPartitionFunction<WithCount<CompressedDFSCode>, GraphTransaction> {

  /**
   * Property key to store a pattern's support.
   */
  public static final String SUPPORT_KEY = "support";
  /**
   * Property key to store a string representation of a DFS code.
   */
  public static final String DFS_CODE_KEY = "dfsCode";

  /**
   * FSM configuration.
   */
  private final FSMConfig fsmConfig;
  /**
   * Factory to create EPGM graph heads.
   */
  private final GraphHeadFactory graphHeadFactory;
  /**
   * Factory to create EPGM vertices.
   */
  private final VertexFactory vertexFactory;
  /**
   * Factory to create EPGM edges.
   */
  private final EdgeFactory edgeFactory;

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   * @param graphHeadFactory graph head factory
   * @param vertexFactory vertex factory
   * @param edgeFactory edge factory
   */
  public DecodeDFSCodes(FSMConfig fsmConfig, GraphHeadFactory graphHeadFactory,
    VertexFactory vertexFactory, EdgeFactory edgeFactory) {

    this.fsmConfig = fsmConfig;
    this.graphHeadFactory = graphHeadFactory;
    this.vertexFactory = vertexFactory;
    this.edgeFactory = edgeFactory;
  }

  @Override
  public void mapPartition(
    Iterable<WithCount<CompressedDFSCode>> frequentSubgraphs,
    Collector<GraphTransaction> out) throws Exception {

    DistributedCacheClient cacheClient =
      DistributedCache.getClient(fsmConfig.getCacheClientConfiguration(),
        fsmConfig.getSession());

    List<String> vertexLabelDictionary = cacheClient.getList(
      Constants.VERTEX_PREFIX + Constants.LABEL_DICTIONARY_INVERSE);

    List<String> edgeLabelDictionary = cacheClient.getList(
      Constants.EDGE_PREFIX + Constants.LABEL_DICTIONARY_INVERSE);

    for (WithCount<CompressedDFSCode> subgraphFrequency : frequentSubgraphs) {
      DFSCode subgraph = subgraphFrequency.getObject().getDfsCode();

      GraphHead graphHead =
        graphHeadFactory.createGraphHead("Frequent SubgraphWithCount");
      GradoopIdSet graphId = GradoopIdSet.fromExisting(graphHead.getId());

      int frequency = subgraphFrequency.getCount();
      long graphCount = cacheClient.getCounter(Constants.GRAPH_COUNT);
      graphHead.setProperty(SUPPORT_KEY, (float) frequency / graphCount);
      graphHead.setProperty(DFS_CODE_KEY, subgraph.toString());

      Set<Vertex> vertices = Sets.newHashSet();
      Map<Integer, GradoopId> vertexTimeIdMap = Maps.newHashMap();
      Set<Edge> edges = Sets.newHashSetWithExpectedSize(subgraph.size());

      for (DFSStep step : subgraph.getSteps()) {

        int sourceTime;
        int targetTime;

        if (step.isOutgoing()) {
          sourceTime = step.getFromTime();
          targetTime = step.getToTime();
        } else {
          sourceTime = step.getToTime();
          targetTime = step.getFromTime();
        }

        GradoopId sourceId = vertexTimeIdMap.get(sourceTime);

        if (sourceId == null) {
          int label =
            step.isOutgoing() ? step.getFromLabel() : step.getToLabel();

          Vertex vertex = vertexFactory
            .createVertex(vertexLabelDictionary.get(label), graphId);

          vertices.add(vertex);
          sourceId = vertex.getId();
          vertexTimeIdMap.put(sourceTime, sourceId);
        }

        GradoopId targetId = vertexTimeIdMap.get(targetTime);

        if (targetId == null) {
          int label =
            step.isOutgoing() ? step.getToLabel() : step.getFromLabel();

          Vertex vertex = vertexFactory
            .createVertex(vertexLabelDictionary.get(label), graphId);

          vertices.add(vertex);
          targetId = vertex.getId();
          vertexTimeIdMap.put(targetTime, targetId);
        }

        String edgeLabel = edgeLabelDictionary.get(step.getEdgeLabel());

        edges.add(edgeFactory
          .createEdge(edgeLabel, sourceId, targetId, graphId));
      }

      out.collect(new GraphTransaction(graphHead, vertices, edges));
    }
  }
}

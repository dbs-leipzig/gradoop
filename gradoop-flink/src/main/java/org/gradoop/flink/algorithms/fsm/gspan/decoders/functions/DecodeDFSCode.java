package org.gradoop.flink.algorithms.fsm.gspan.decoders.functions;

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

public class DecodeDFSCode extends
  RichMapPartitionFunction<WithCount<CompressedDFSCode>, GraphTransaction> {

  private final FSMConfig fsmConfig;
  private final GraphHeadFactory graphHeadFactory;
  private final VertexFactory vertexFactory;
  private final EdgeFactory edgeFactory;

  public DecodeDFSCode(FSMConfig fsmConfig, GraphHeadFactory graphHeadFactory,
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
      DistributedCache.getClient(fsmConfig.getCacheServerAddress());

    List<String> vertexLabelDictionary = cacheClient.getList(
      Constants.VERTEX_PREFIX + Constants.LABEL_DICTIONARY_INVERSE);

    List<String> edgeLabelDictionary = cacheClient.getList(
      Constants.EDGE_PREFIX + Constants.LABEL_DICTIONARY_INVERSE);

    for (WithCount<CompressedDFSCode> subgraphFrequency : frequentSubgraphs) {
      DFSCode subgraph = subgraphFrequency.getObject().getDfsCode();

      GraphHead graphHead =
        graphHeadFactory.createGraphHead("Frequent Subgraph");
      GradoopIdSet graphId = GradoopIdSet.fromExisting(graphHead.getId());

      int frequency = subgraphFrequency.getCount();
      long graphCount = cacheClient.getCounter(Constants.GRAPH_COUNT);
      graphHead.setProperty("support", (float) frequency / graphCount);
<<<<<<< HEAD
      graphHead.setProperty("dfsCode", subgraph.toString());
=======
>>>>>>> origin/#292_distcache

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

package org.gradoop.flink.algorithms.fsm.gspan.miners;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyList;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyListEntry;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSEmbedding;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DirectedDFSStep;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.List;
import java.util.Map;


public class CacheBasedGSpan
  extends RichMapPartitionFunction<GSpanGraph, Object> {

  private final FSMConfig fsmConfig;
  private List<GSpanGraph> graphs = Lists.newArrayList();
  private Map<DFSCode, Map<Integer, Collection<DFSEmbedding>>>
    subgraphGraphEmbeddings = Maps.newHashMap();
  private DistributedCacheClient cacheClient;
  private int partition;
  private int partitionCount;
  private long minFrequency;
  private Collection<WithCount<CompressedDFSCode>> frequentSubgraphStore =
    Lists.newArrayList();

  public CacheBasedGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(
    Iterable<GSpanGraph> values, Collector<Object> out) throws Exception {
    this.partition = getRuntimeContext().getIndexOfThisSubtask();
    this.partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    this.cacheClient =
      DistributedCache.getClient(fsmConfig.getCacheServerAddress());
    this.minFrequency = cacheClient.getCounter(Constants.MIN_FREQUENCY);

    createDirectedSingleEdgeSubgraphs(values);

    reportSubgraphs(1);
    countAndFilterSubgraphs(1);

    for (WithCount<CompressedDFSCode> frequentSubgraph : this.frequentSubgraphStore) {
      out.collect(frequentSubgraph);
    }
  }

  private void countAndFilterSubgraphs(int k) throws InterruptedException {
    Map<CompressedDFSCode, Integer> subgraphGlobalFrequencies =
      Maps.newHashMap();

    Collection<CompressedDFSCode> frequentSubgraphs = Lists.newArrayList();

    List<WithCount<CompressedDFSCode>> subgraphLocalFrequencies =
      cacheClient.getList(String.valueOf(partition));

    for (WithCount<CompressedDFSCode> subgraphLocalFrequency :
      subgraphLocalFrequencies) {

      CompressedDFSCode subgraph = subgraphLocalFrequency.getObject();
      Integer localFrequency = subgraphLocalFrequency.getCount();
      Integer globalFrequency = subgraphGlobalFrequencies.get(subgraph);

      if (globalFrequency == null) {
        globalFrequency = localFrequency;
      } else {
        globalFrequency += localFrequency;
      }

      subgraphGlobalFrequencies.put(subgraph, globalFrequency);
    }

    for (Map.Entry<CompressedDFSCode, Integer> subgraphGlobalFrequency :
         subgraphGlobalFrequencies.entrySet()) {

      CompressedDFSCode subgraph = subgraphGlobalFrequency.getKey();
      int frequency = subgraphGlobalFrequency.getValue();

      if (frequency >= minFrequency) {
        frequentSubgraphs.add(subgraph);
        frequentSubgraphStore.add(new WithCount<>(subgraph, frequency));
      }
    }

    String counterName = "ak" + k;
    cacheClient.incrementAndGetCounter(counterName);
    cacheClient.waitForCounterToReach(counterName, partitionCount);
  }

  private void reportSubgraphs(int k) throws InterruptedException {
    Map<Integer, Collection<WithCount<CompressedDFSCode>>>
      partitionSubgraphFrequencies =
      Maps.newHashMapWithExpectedSize(partitionCount);

    for (Map.Entry<DFSCode, Map<Integer, Collection<DFSEmbedding>>> entry :
    subgraphGraphEmbeddings.entrySet()) {

      DFSCode subgraph = entry.getKey();

      if (GSpan.isMinimal(subgraph, fsmConfig)) {
        int aggPartition = subgraph.hashCode() % partitionCount;

        int frequency = entry.getValue().size();

        WithCount<CompressedDFSCode> subgraphFrequency =
          new WithCount<>(new CompressedDFSCode(subgraph), frequency);

        Collection<WithCount<CompressedDFSCode>> subgraphFrequencies =
          partitionSubgraphFrequencies.get(aggPartition);
        
        if (subgraphFrequencies == null) {
          //noinspection unchecked
          subgraphFrequencies = Lists.newArrayList(subgraphFrequency);
          partitionSubgraphFrequencies.put(aggPartition, subgraphFrequencies);
        } else {
          subgraphFrequencies.add(subgraphFrequency);
        }
      }
    }
    
    for (Map.Entry<Integer, Collection<WithCount<CompressedDFSCode>>> entry :
      partitionSubgraphFrequencies.entrySet()) {
      String aggPartition = String.valueOf(entry.getKey());
      Collection<WithCount<CompressedDFSCode>> subgraphs = entry.getValue();

      cacheClient.getList(aggPartition).addAll(subgraphs);
    }

    String counterName = "rk" + k;
    cacheClient.incrementAndGetCounter(counterName);
    cacheClient.waitForCounterToReach(counterName, partitionCount);
  }

  private void createDirectedSingleEdgeSubgraphs(Iterable<GSpanGraph> values) {
    int graphId = 0;
    for (GSpanGraph graph : values) {
      graphs.add(graph);

      Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings =
        Maps.newHashMap();

      for (Map.Entry<Integer, AdjacencyList> vertexIdAdjacencyList :
        graph.getAdjacencyLists().entrySet()) {

        AdjacencyList adjacencyList = vertexIdAdjacencyList.getValue();

        int fromLabel = adjacencyList.getFromVertexLabel();
        int fromId = vertexIdAdjacencyList.getKey();

        for (AdjacencyListEntry entry : adjacencyList.getEntries()) {
          boolean outgoing = entry.isOutgoing();
          int toLabel = entry.getToVertexLabel();

          if (fromLabel < toLabel || fromLabel == toLabel && outgoing) {
            int edgeLabel = entry.getEdgeLabel();
            int toId = entry.getToVertexId();
            boolean loop = fromId == toId;
            int toTime = loop ? 0 : 1;
            int edgeId = entry.getEdgeId();

            DFSCode subgraph = new DFSCode(new DirectedDFSStep(
              0, toTime, fromLabel, outgoing, edgeLabel, toLabel));

            DFSEmbedding embedding = new DFSEmbedding(
              loop ?
                Lists.newArrayList(fromId) :
                Lists.newArrayList(fromId, toId),
              Lists.newArrayList(edgeId)
            );

            Collection<DFSEmbedding> embeddings =
              subgraphEmbeddings.get(subgraph);

            if (embeddings == null) {
              subgraphEmbeddings.put(
                subgraph, Lists.newArrayList(embedding));
            } else {
              embeddings.add(embedding);
            }
          }
        }
      }

      for (Map.Entry<DFSCode, Collection<DFSEmbedding>> entry :
        subgraphEmbeddings.entrySet()) {

        DFSCode subgraph = entry.getKey();
        Collection<DFSEmbedding> embeddings = entry.getValue();

        Map<Integer, Collection<DFSEmbedding>> graphIdEmbeddings =
          subgraphGraphEmbeddings.get(subgraph);

        if (graphIdEmbeddings == null) {
          graphIdEmbeddings = Maps.newHashMap();
          subgraphGraphEmbeddings.put(subgraph, graphIdEmbeddings);
        }

        graphIdEmbeddings.put(graphId, embeddings);
      }

      graphId++;
    }
  }
}

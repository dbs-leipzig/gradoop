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
import java.util.Iterator;
import java.util.List;
import java.util.Map;


public class CacheBasedGSpan
  extends RichMapPartitionFunction<GSpanGraph, WithCount<CompressedDFSCode>> {

  private final FSMConfig fsmConfig;
  private List<GSpanGraph> graphs = Lists.newLinkedList();
  private DistributedCacheClient cacheClient;
  private int partition;
  private int partitionCount;
  private long minFrequency;
  private Collection<WithCount<CompressedDFSCode>> frequentSubgraphs =
    Lists.newArrayList();

  public CacheBasedGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(Iterable<GSpanGraph> values,
    Collector<WithCount<CompressedDFSCode>> out) throws Exception {

    this.partition = getRuntimeContext().getIndexOfThisSubtask();
    this.partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    this.cacheClient =
      DistributedCache.getClient(fsmConfig.getCacheClientConfiguration());
    this.minFrequency = cacheClient.getCounter(Constants.MIN_FREQUENCY);

    Collection<CompressedDFSCode> frequentSubgraphs;

    // single edge subgraphs
    int k = 1;
    createDirectedSingleEdgeSubgraphs(values);
    reportSubgraphs(k);
    countAndFilterSubgraphs(k, out);
    frequentSubgraphs = cacheClient.getList(Constants.FREQUENT_SUBGRAPHS);

    // k-edge subgraphs
    while (! frequentSubgraphs.isEmpty()) {
      k++;
      growChildren(frequentSubgraphs);
      reportSubgraphs(k);
      countAndFilterSubgraphs(k, out);
      frequentSubgraphs = cacheClient.getList(Constants.FREQUENT_SUBGRAPHS);
    }

    if (fsmConfig.isVerbose()) {
      System.out.println(
        "Partition " + partition + "/" + partitionCount + "finished");
    }

    for (WithCount<CompressedDFSCode> frequentSubgraph :
      this.frequentSubgraphs) {

      out.collect(frequentSubgraph);
    }
  }

  private void growChildren(Collection<CompressedDFSCode> compressedSubgraphs
  ) throws InterruptedException {

    // decompress frequent subgraphs
    Collection<DFSCode> frequentSubgraphs = Lists
      .newArrayListWithExpectedSize(compressedSubgraphs.size());

    for (CompressedDFSCode compressedSubgraph : compressedSubgraphs) {
      frequentSubgraphs.add(compressedSubgraph.getDfsCode());
    }

    // grow embeddings and drop graphs without successful growths

    Iterator<GSpanGraph> graphIterator = graphs.iterator();

    while (graphIterator.hasNext()) {
      GSpanGraph graph = graphIterator.next();
      GSpan.growEmbeddings(graph, frequentSubgraphs, fsmConfig);

      if (! graph.hasGrownSubgraphs()) {
        graphIterator.remove();
      }
    }
  }

  private void countAndFilterSubgraphs(int k,
    Collector< WithCount<CompressedDFSCode>> out) throws InterruptedException {

    String eventName = "clear" + k;

    if (partition == 0) {
      cacheClient.getList(Constants.FREQUENT_SUBGRAPHS).clear();
      cacheClient.triggerEvent(eventName);
    } else {
      cacheClient.waitForEvent(eventName);
    }

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
        this.frequentSubgraphs.add(new WithCount<>(subgraph, frequency));
      }
    }

    String counterName = "count" + k;
    cacheClient.getList(Constants.FREQUENT_SUBGRAPHS).addAll(frequentSubgraphs);

    if (fsmConfig.isVerbose()) {
      System.out.println(partition + ":" + frequentSubgraphs);
    }

    cacheClient.setList(String.valueOf(partition), Lists.newArrayList());
    cacheClient.incrementAndGetCounter(counterName);
    cacheClient.waitForCounterToReach(counterName, partitionCount);
  }

  private void reportSubgraphs(int k) throws InterruptedException {

    // count local frequency

    Map<DFSCode, Integer> subgraphLocalFrequencies = Maps.newHashMap();

    for (GSpanGraph graph : graphs) {
      for (DFSCode subgraph : graph.getSubgraphEmbeddings().keySet()) {
        Integer localFrequency = subgraphLocalFrequencies.get(subgraph);

        if (localFrequency == null) {
          localFrequency = 1;
        } else {
          localFrequency += 1;
        }

        subgraphLocalFrequencies.put(subgraph, localFrequency);
      }
    }

    // partition local frequencies and validate subgraphs

    Map<Integer, Collection<WithCount<CompressedDFSCode>>>
      partitionSubgraphFrequencies =
      Maps.newHashMapWithExpectedSize(partitionCount);

    for (Map.Entry<DFSCode, Integer> entry :
      subgraphLocalFrequencies.entrySet()) {

      DFSCode subgraph = entry.getKey();

      if (GSpan.isMinimal(subgraph, fsmConfig)) {
        int aggPartition = subgraph.hashCode() % partitionCount;
        aggPartition = aggPartition < 0 ? aggPartition * -1 : aggPartition;

        int frequency = entry.getValue();

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

      if (fsmConfig.isVerbose()) {
        System.out.println(partition + "->" + aggPartition + ":" + subgraphs);
      }

      cacheClient.getList(aggPartition).addAll(subgraphs);
    }

    String counterName = "report" + k;
    cacheClient.incrementAndGetCounter(counterName);
    cacheClient.waitForCounterToReach(counterName, partitionCount);
  }

  private void createDirectedSingleEdgeSubgraphs(Iterable<GSpanGraph> values) {
    for (GSpanGraph graph : values) {
      graphs.add(graph);

      Map<DFSCode, Collection<DFSEmbedding>> subgraphEmbeddings =
        graph.getSubgraphEmbeddings();

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
    }
  }
}

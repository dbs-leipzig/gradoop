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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.GSpan;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.CompressedDFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.DFSCode;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Executed the gSpan algorithm for a partition of graphs.
 */
public class IterativeGSpan
  extends RichMapPartitionFunction<GSpanGraph, WithCount<CompressedDFSCode>> {

  /**
   * FSM configuration.
   */
  private final FSMConfig fsmConfig;
  /**
   * Own partition identifier.
   */
  private int partition;
  /**
   * Count of all partitions.
   */
  private int partitionCount;
  /**
   * Gradoop distributed cache Client.
   */
  private DistributedCacheClient cacheClient;
  /**
   * Minimum frequency of a pattern to be considered to be frequent.
   */
  private long minFrequency;
  /**
   * Partition of gSpan encoded graphs.
   */
  private List<GSpanGraph> graphs = Lists.newLinkedList();
  /**
   * List of frequent subgraph patterns.
   */
  private Collection<WithCount<CompressedDFSCode>> resultPartition =
    Lists.newArrayList();

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   */
  public IterativeGSpan(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(Iterable<GSpanGraph> values,
    Collector<WithCount<CompressedDFSCode>> out) throws Exception {

    this.partition = getRuntimeContext().getIndexOfThisSubtask();
    this.partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    this.cacheClient = null;
    this.minFrequency = cacheClient.getCounter(Constants.MIN_FREQUENCY);

    Collection<CompressedDFSCode> frequentSubgraphs;

    // single edge subgraphs
    int k = 1;
    discoverSingleEdgeSubgraphs(values);
    reportSubgraphs(k);
    countAndFilterSubgraphs(k);
    frequentSubgraphs = cacheClient.getList(Constants.FREQUENT_SUBGRAPHS);

    // k-edge subgraphs
    while (! frequentSubgraphs.isEmpty()) {
      k++;
      growChildren(frequentSubgraphs);
      reportSubgraphs(k);
      countAndFilterSubgraphs(k);
      frequentSubgraphs = cacheClient.getList(Constants.FREQUENT_SUBGRAPHS);
    }

    if (fsmConfig.isVerbose()) {
      System.out.println(
        "Partition " + partition + "/" + partitionCount + "finished");
    }

    for (WithCount<CompressedDFSCode> frequentSubgraph :
      this.resultPartition) {

      out.collect(frequentSubgraph);
    }
  }

  /**
   * Grow k+1-edge embeddings of frequent subgraphs.
   *
   * @param compressedSubgraphs compressed k-edge frequent subgraphs
   * @throws InterruptedException
   */
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

  /**
   * Aggregate partition of k-edge pattern frequencies.
   *
   * @param k edge count
   *
   * @throws InterruptedException
   */
  private void countAndFilterSubgraphs(int k) throws InterruptedException {

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
        this.resultPartition.add(new WithCount<>(subgraph, frequency));
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

  /**
   * Determine local frequency of k-edge patterns and write them to cache.
   *
   * @param k edge count
   * @throws InterruptedException
   */
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

      if (k == 1 || GSpan.isMinimal(subgraph, fsmConfig)) {
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

  /**
   * Determine 1-edge DFS code and embedding for every edge.
   * Store graph to local graph partition.
   *
   * @param values input graphs
   */
  private void discoverSingleEdgeSubgraphs(Iterable<GSpanGraph> values) {
    for (GSpanGraph graph : values) {
      GSpan.createSingleEdgeDfsCodes(graph, fsmConfig);
      graphs.add(graph);
    }
  }
}

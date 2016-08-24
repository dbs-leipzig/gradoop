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
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.comparators.LabelFrequencyEntryComparator;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyList;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyListEntry;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Turns a partition of TLF graphs into graphs encoded for thew gSpan algorithm.
 */
public class EncodeTLFGraphs
  extends RichMapPartitionFunction<TLFGraph, GSpanGraph> {

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
   * Vertex label dictionary.
   */
  private Map<String, Integer> vertexLabelDictionary;
  /**
   * Edge label dictionary.
   */
  private Map<String, Integer> edgeLabelDictionary;
  /**
   * Partition of graph transactions.
   */
  private Collection<TLFGraph> graphs;
  /**
   * Local cache for graph-local vertex adjacency lists.
   */
  private Collection<Map<Integer, AdjacencyList>> graphAdjacencyLists;

  /**
   * Constructor.
   * @param fsmConfig FSM configuration
   */
  public EncodeTLFGraphs(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(
    Iterable<TLFGraph> values, Collector<GSpanGraph> out) throws Exception {

    graphs = Lists.newArrayList();
    partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    partition = getRuntimeContext().getIndexOfThisSubtask();
    cacheClient = DistributedCache.getClient(
      fsmConfig.getCacheClientConfiguration(), fsmConfig.getSession());

    long reporter = reportVertexLabelFrequency(values);
    if (reporter == 1) {
      setMinFrequency();
      vertexLabelDictionary = createLabelDictionary(Constants.VERTEX_PREFIX);
    } else {
      vertexLabelDictionary = readLabelDictionary(Constants.VERTEX_PREFIX);
      readMinFrequency();
    }

    reporter = reportEdgeLabels();
    if (reporter == 1) {
      edgeLabelDictionary = createLabelDictionary(Constants.EDGE_PREFIX);
    } else {
      edgeLabelDictionary = readLabelDictionary(Constants.EDGE_PREFIX);
    }

    encodeGraphs();

    for (Map<Integer, AdjacencyList> adjacencyLists : graphAdjacencyLists) {
      if (!adjacencyLists.isEmpty()) {
        out.collect(new GSpanGraph(adjacencyLists));
      }
    }
  }

  /**
   * Read the global graph count from cache, calculate minimum frequency
   * based on the configured minimum support and write the minimum frequency
   * to cache.
   *
   * @throws InterruptedException
   */
  private void setMinFrequency() throws InterruptedException {
    cacheClient.waitForCounterToReach(
      Constants.GRAPH_COUNT_REPORTS, partitionCount);

    this.minFrequency = Math.round((float) cacheClient
      .getCounter(Constants.GRAPH_COUNT) * fsmConfig.getMinSupport());

    cacheClient.setCounter(Constants.MIN_FREQUENCY, minFrequency);
  }

  /**
   * Read minimum frequency from cache and set local field.
   */
  private void readMinFrequency() {
    this.minFrequency = cacheClient.getCounter(Constants.MIN_FREQUENCY);
  }

  /**
   * Writes local graph count and vertex label frequency to distributed cache.
   *
   * @param graphs local graphs
   * @return number of vertex label frequency report
   * @throws InterruptedException
   */
  private long reportVertexLabelFrequency(Iterable<TLFGraph> graphs) throws
    InterruptedException {

    Map<String, Integer> labelFrequency = Maps.newHashMap();

    long graphCount = 0;

    for (TLFGraph graph : graphs) {
      graphCount++;
      this.graphs.add(graph);

      Set<String> vertexLabels = Sets.newHashSet();

      for (TLFVertex vertex : graph.getVertices()) {
        vertexLabels.add(vertex.getLabel());
      }

      addLabelFrequency(labelFrequency, vertexLabels);
    }

    cacheClient.addAndGetCounter(Constants.GRAPH_COUNT, graphCount);
    cacheClient.incrementAndGetCounter(Constants.GRAPH_COUNT_REPORTS);

    cacheClient.setMap(String.valueOf(partition), labelFrequency);

    return cacheClient
      .incrementAndGetCounter(Constants.TASK_FINISHED);
  }

  /**
   * Determine local frequency of edge labels and write result to
   * distributed cache.
   *
   * @return number of report
   */
  private long reportEdgeLabels() {
    Map<String, Integer> labelFrequency = Maps.newHashMap();

    graphAdjacencyLists = Lists.newArrayListWithCapacity(graphs.size());

    for (TLFGraph graph : graphs) {
      Map<Integer, AdjacencyList> vertexAdjacencyLists = Maps.newHashMap();
      graphAdjacencyLists.add(vertexAdjacencyLists);
      Collection<String> edgeLabels = Sets.newHashSet();

      // drop vertices with infrequent labels
      Iterator<TLFVertex> vertexIterator = graph.getVertices().iterator();

      while (vertexIterator.hasNext()) {
        TLFVertex vertex = vertexIterator.next();

        Integer vertexLabel = vertexLabelDictionary.get(vertex.getLabel());

        if (vertexLabel != null) {
          vertexAdjacencyLists.put(
            vertex.getId(), new AdjacencyList(vertexLabel));
        } else {
          vertexIterator.remove();
        }
      }

      // drop edges with dropped vertices and report edge labels

      Iterator<TLFEdge> edgeIterator = graph.getEdges().iterator();

      while (edgeIterator.hasNext()) {
        TLFEdge edge = edgeIterator.next();

        if (vertexAdjacencyLists.containsKey(edge.getSourceId()) &&
          vertexAdjacencyLists.containsKey(edge.getTargetId())) {
          edgeLabels.add(edge.getLabel());
        } else {
          edgeIterator.remove();
        }
      }

      addLabelFrequency(labelFrequency, edgeLabels);
    }

    cacheClient.setMap(String.valueOf(partition), labelFrequency);

    return cacheClient
      .incrementAndGetCounter(Constants.TASK_FINISHED);
  }

  /**
   * Increase frequency fro a given set of labels in a label frequency map.
   *
   * @param labelFrequency label frequency map
   * @param labels labels
   */
  private void addLabelFrequency(
    Map<String, Integer> labelFrequency, Collection<String> labels) {

    for (String label : labels) {
      Integer frequency = labelFrequency.get(label);

      if (frequency == null) {
        frequency = 1;
      } else {
        frequency += 1;
      }

      labelFrequency.put(label, frequency);
    }
  }

  /**
   * Read label frequencies of all partitions, getVertexIncrement their global
   * frequency, filter frequent ones and create a label dictionary including
   * its inverse dictionary. Write both to cache.
   *
   * @param prefix vertex or edge prefix for cache keys
   * @return dictionary
   * @throws InterruptedException
   */
  private Map<String, Integer> createLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForCounterToReach(Constants.TASK_FINISHED, partitionCount);

    Map<String, Integer> labelFrequencies = Maps.newHashMap();

    for (int i = 0; i < partitionCount; i++) {
      Map<String, Integer> report = cacheClient.getMap(String.valueOf(i));

      for (Map.Entry<String, Integer> labelFrequency : report.entrySet()) {

        String label = labelFrequency.getKey();
        Integer frequency = labelFrequency.getValue();

        Integer sum = labelFrequencies.get(label);

        if (sum == null) {
          sum = frequency;
        } else {
          sum += frequency;
        }

        labelFrequencies.put(label, sum);
      }

      cacheClient.delete(String.valueOf(i));
    }

    List<Map.Entry<String, Integer>> labelsWithCount = Lists
      .newArrayList(labelFrequencies.entrySet());

    Collections.sort(labelsWithCount, new LabelFrequencyEntryComparator());

    Map<String, Integer> dictionary = Maps
      .newHashMapWithExpectedSize(labelsWithCount.size());
    List<String> inverseDictionary = Lists
      .newArrayListWithCapacity(labelsWithCount.size());

    Integer translation = 0;
    for (Map.Entry<String, Integer> entry : labelsWithCount) {
      String label = entry.getKey();
      Integer frequency = entry.getValue();

      if (frequency >= minFrequency) {
        dictionary.put(label, translation);
        inverseDictionary.add(label);

        translation++;
      }
    }

    cacheClient.setMap(prefix + Constants.LABEL_DICTIONARY, dictionary);
    cacheClient.triggerEvent(prefix + Constants.LABEL_DICTIONARY_AVAILABLE);
    cacheClient.setList(
      prefix + Constants.LABEL_DICTIONARY_INVERSE, inverseDictionary);

    cacheClient.resetCounter(Constants.TASK_FINISHED);

    return dictionary;
  }

  /**
   * Read a label dictionary from distributed cache.
   *
   * @param prefix vertex/edge
   * @return label dictionary
   *
   * @throws InterruptedException
   */
  private Map<String, Integer> readLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForEvent(prefix + Constants.LABEL_DICTIONARY_AVAILABLE);
    return cacheClient.getMap(prefix + Constants.LABEL_DICTIONARY);
  }

  /**
   * Use previously generated vertex Id mappings and encode labels to
   * generate the gSpan graph representation.
   */
  private void encodeGraphs() {

    Iterator<TLFGraph> graphIterator = graphs.iterator();
    Iterator<Map<Integer, AdjacencyList>> adjacencyListsIterator =
      graphAdjacencyLists.iterator();

    while (graphIterator.hasNext()) {
      TLFGraph graph = graphIterator.next();
      Map<Integer, AdjacencyList> adjacencyLists =
        adjacencyListsIterator.next();

      int edgeId = 0;

      for (TLFEdge edge : graph.getEdges()) {
        Integer edgeLabel = edgeLabelDictionary.get(edge.getLabel());

        if (edgeLabel != null) {
          Integer sourceId = edge.getSourceId();
          AdjacencyList sourceAdjacencyList = adjacencyLists.get(sourceId);
          Integer sourceLabel = sourceAdjacencyList.getFromVertexLabel();

          Integer targetId = edge.getTargetId();
          AdjacencyList targetAdjacencyList = adjacencyLists.get(targetId);
          Integer targetLabel = targetAdjacencyList.getFromVertexLabel();

          sourceAdjacencyList.getEntries().add(new AdjacencyListEntry(
            true, edgeId, edgeLabel, targetId, targetLabel));

          targetAdjacencyList.getEntries().add(new AdjacencyListEntry(
            !fsmConfig.isDirected(), edgeId, edgeLabel, sourceId, sourceLabel));

          edgeId++;
        }
      }

      Iterator<Map.Entry<Integer, AdjacencyList>> vertexAdjacencyListIterator
        = adjacencyLists.entrySet().iterator();

      while (vertexAdjacencyListIterator.hasNext()) {
        if (vertexAdjacencyListIterator.next().getValue()
          .getEntries().isEmpty()) {
          vertexAdjacencyListIterator.remove();
        }
      }
    }
  }
}

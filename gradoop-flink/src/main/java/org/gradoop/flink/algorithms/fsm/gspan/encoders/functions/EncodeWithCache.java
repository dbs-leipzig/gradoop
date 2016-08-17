package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.comparators
  .LabelFrequencyEntryComparator;
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

public class EncodeWithCache
  extends RichMapPartitionFunction<TLFGraph, GSpanGraph> {

  private final FSMConfig fsmConfig;
  private Collection<TLFGraph> graphs;
  private int partitionCount;
  private DistributedCacheClient cacheClient;
  private long minFrequency;
  private int partition;

  public EncodeWithCache(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(
    Iterable<TLFGraph> values, Collector<GSpanGraph> out) throws Exception {

    setLocalFields();

    long reporter = reportVertexLabelFrequency(values);
    Map<String, Integer> vertexLabelDictionary;
    if (reporter == 1) {
      setMinFrequency();
      vertexLabelDictionary = createLabelDictionary(Constants.VERTEX_PREFIX);
    } else {
      vertexLabelDictionary = readLabelDictionary(Constants.VERTEX_PREFIX);
      readMinFrequency();
    }

    reporter = reportEdgeLabels(vertexLabelDictionary);
    Map<String, Integer> edgeLabelDictionary;
    if (reporter == 1) {
      edgeLabelDictionary = createLabelDictionary(Constants.EDGE_PREFIX);
    } else {
      edgeLabelDictionary = readLabelDictionary(Constants.EDGE_PREFIX);
    }

    for (TLFGraph graph : graphs) {
      Map<Integer, AdjacencyList> adjacencyLists =
        encodeGraphs(graph, vertexLabelDictionary, edgeLabelDictionary);

      if (! adjacencyLists.isEmpty()) {
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
    cacheClient.waitForCounterToReach(Constants.GRAPH_COUNT_REPORTS, partitionCount);

    this.minFrequency = Math.round((float)
      cacheClient.getCounter(Constants.GRAPH_COUNT) * fsmConfig.getMinSupport());

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

      for (TLFVertex vertex : graph.getGraphVertices()) {
        vertexLabels.add(vertex.getLabel());
      }

      addLabelFrequency(labelFrequency, vertexLabels);
    }

    cacheClient.addAndGetCounter(Constants.GRAPH_COUNT, graphCount);
    cacheClient.incrementAndGetCounter(Constants.GRAPH_COUNT_REPORTS);

    cacheClient.setMap(String.valueOf(partition), labelFrequency);

    return cacheClient
      .incrementAndGetCounter(Constants.REPORTS);
  }

  private long reportEdgeLabels(Map<String, Integer> vertexLabelDictionary) {
    Map<String, Integer> labelFrequency = Maps.newHashMap();

    for (TLFGraph graph : graphs) {
      Collection<Integer> vertexIdsWithFrequentLabels = Sets.newHashSet();
      Collection<String> edgeLabels = Sets.newHashSet();

      // drop vertices with infrequent labels
      for (TLFVertex vertex : graph.getGraphVertices()) {
        if (vertexLabelDictionary.containsKey(vertex.getLabel())) {
          vertexIdsWithFrequentLabels.add(vertex.getId());
        }
      }

      // drop edges with dropped vertices and report edge labels

      Iterator<TLFEdge> edgeIterator = graph.getEdges().iterator();

      while (edgeIterator.hasNext()) {
        TLFEdge edge = edgeIterator.next();
        if (vertexIdsWithFrequentLabels.contains(edge.getSourceId()) &&
          vertexIdsWithFrequentLabels.contains(edge.getTargetId())) {
          edgeLabels.add(edge.getLabel());
        } else {
          edgeIterator.remove();
        }
      }

      addLabelFrequency(labelFrequency, edgeLabels);
    }

    cacheClient.setMap(String.valueOf(partition), labelFrequency);

    return cacheClient
      .incrementAndGetCounter(Constants.REPORTS);
  }

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
   * Read label frequencies of all partitions, aggregate their global
   * frequency, filter frequent ones and create a label dictionary including
   * its inverse dictionary. Write both to cache.
   *
   * @param prefix vertex or edge prefix for cache keys
   * @return dictionary
   * @throws InterruptedException
   */
  private Map<String, Integer> createLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForCounterToReach(Constants.REPORTS, partitionCount);

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

    cacheClient.resetCounter(Constants.REPORTS);

    return dictionary;
  }

  private Map<String, Integer> readLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForEvent(prefix + Constants.LABEL_DICTIONARY_AVAILABLE);
    return cacheClient.getMap(prefix + Constants.LABEL_DICTIONARY);
  }

  private Map<Integer, AdjacencyList> encodeGraphs(TLFGraph graph,
    Map<String, Integer> vertexLabelDictionary,
    Map<String, Integer> edgeLabelDictionary) {
    Map<Integer, AdjacencyList> adjacencyLists = Maps.newHashMap();
    int edgeId = 0;

    for (TLFEdge edge : graph.getEdges()) {
      Integer edgeLabel = edgeLabelDictionary.get(edge.getLabel());

      if (edgeLabel != null) {
        edgeId ++;

        Integer sourceId = edge.getSourceId();
        Integer sourceLabel;
        Integer targetId = edge.getTargetId();
        Integer targetLabel;

        AdjacencyList sourceAdjacencyList = adjacencyLists.get(sourceId);
        if (sourceAdjacencyList == null) {
          sourceLabel = vertexLabelDictionary
            .get(graph.getGraphVertices().get(sourceId).getLabel());

          sourceAdjacencyList = new AdjacencyList(sourceLabel);
          adjacencyLists.put(sourceId, sourceAdjacencyList);
        } else {
          sourceLabel = sourceAdjacencyList.getFromVertexLabel();
        }

        AdjacencyList targetAdjacencyList = adjacencyLists.get(targetId);
        if (targetAdjacencyList == null) {
          targetLabel = vertexLabelDictionary
            .get(graph.getGraphVertices().get(targetId).getLabel());

          targetAdjacencyList = new AdjacencyList(targetLabel);
          adjacencyLists.put(sourceId, targetAdjacencyList);
        } else {
          targetLabel = targetAdjacencyList.getFromVertexLabel();
        }

        sourceAdjacencyList.getEntries().add(new AdjacencyListEntry(
          true, edgeId, edgeLabel, targetId, targetLabel));

        targetAdjacencyList.getEntries().add(new AdjacencyListEntry(
          !fsmConfig.isDirected(), edgeId, edgeLabel, sourceId, sourceLabel));
      }
    }
    return adjacencyLists;
  }

  private void setLocalFields() {
    cacheClient = DistributedCache.getClient(fsmConfig.getCacheServerAddress());
    graphs = Lists.newArrayList();
    partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    partition = getRuntimeContext().getIndexOfThisSubtask();
  }

}

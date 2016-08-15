package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
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

  public static final String VERTEX_PREFIX = "v";
  public static final String EDGE_PREFIX = "e";

  public static final String LABEL_FREQUENCIES = "lfs";
  public static final String LABEL_FREQUENCY_REPORTS = "vlfc";
  public static final String LABEL_DICTIONARY = "vld";
  public static final String LABEL_DICTIONARY_INVERSE = "vldi";
  public static final String LABEL_DICTIONARY_AVAILABLE = "vlda";
  private static final String GRAPH_COUNT = "gc";
  private static final String GRAPH_COUNT_REPORTS = "gcr";
  private static final String MIN_FREQUENCY = "mf";

  private final FSMConfig fsmConfig;
  private Collection<TLFGraph> graphs;
  private int partitionCount;
  private DistributedCacheClient cacheClient;
  private long minGlobalFrequency;
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
      setGlobalMinFrequency();
      vertexLabelDictionary = createLabelDictionary(VERTEX_PREFIX);
    } else {
      vertexLabelDictionary = readLabelDictionary(VERTEX_PREFIX);
      readGlobalMinFrequency();
    }

    reporter = reportEdgeLabels(vertexLabelDictionary);
    Map<String, Integer> edgeLabelDictionary;
    if (reporter == 1) {
      edgeLabelDictionary = createLabelDictionary(EDGE_PREFIX);
    } else {
      edgeLabelDictionary = readLabelDictionary(EDGE_PREFIX);
    }

    for (TLFGraph graph : graphs) {
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

      if (! adjacencyLists.isEmpty()) {
        out.collect(new GSpanGraph(adjacencyLists));
      }
    }
  }

  private void readGlobalMinFrequency() {
    this.minGlobalFrequency = cacheClient.getCounter(MIN_FREQUENCY);
  }

  private void setGlobalMinFrequency() throws InterruptedException {
    cacheClient.waitForCounterToReach(GRAPH_COUNT_REPORTS, partitionCount);

    this.minGlobalFrequency = Math.round((float)
    cacheClient.getCounter(GRAPH_COUNT) * fsmConfig.getMinSupport());

    cacheClient.setCounter(MIN_FREQUENCY, minGlobalFrequency);
  }

  private long reportEdgeLabels(Map<String, Integer> vertexLabelDictionary) {
    Map<String, Integer> labelFrequencies = Maps.newHashMap();

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

      addLabelFrequency(labelFrequencies, edgeLabels);
    }

    cacheClient.getList(LABEL_FREQUENCIES).add(labelFrequencies);

    return cacheClient
      .incrementAndGetCounter(LABEL_FREQUENCY_REPORTS);
  }

  private Map<String, Integer> readLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForEvent(prefix + LABEL_DICTIONARY_AVAILABLE);
    return cacheClient.getMap(prefix + LABEL_DICTIONARY);
  }

  private Map<String, Integer> createLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForCounterToReach(LABEL_FREQUENCY_REPORTS, partitionCount);

    Map<String, Integer> labelFrequencies = Maps.newHashMap();

    Collection<Map<String, Integer>> frequencyReports = cacheClient
      .getList(LABEL_FREQUENCIES);

    for (Map<String, Integer> frequencyReport : frequencyReports) {
      for (Map.Entry<String, Integer> labelFrequency :
        frequencyReport.entrySet()) {

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

      if (frequency >= minGlobalFrequency) {
        dictionary.put(label, translation);
        inverseDictionary.add(label);

        translation++;
      }
    }

    cacheClient.setMap(prefix + LABEL_DICTIONARY, dictionary);
    cacheClient.triggerEvent(prefix + LABEL_DICTIONARY_AVAILABLE);
    cacheClient.setList(prefix + LABEL_DICTIONARY_INVERSE, inverseDictionary);

    cacheClient.resetCounter(LABEL_FREQUENCY_REPORTS);
    cacheClient.getList(LABEL_FREQUENCIES).clear();

    return dictionary;
  }

  private long reportVertexLabelFrequency(Iterable<TLFGraph> values) throws
    InterruptedException {
    Set<String> vertexLabels = Sets.newHashSet();
    Map<String, Integer> vertexLabelFrequency = Maps.newHashMap();

    long graphCount = 0;

    for (TLFGraph graph : values) {
      graphCount++;
      graphs.add(graph);
      vertexLabels.clear();

      for (TLFVertex vertex : graph.getGraphVertices()) {
        vertexLabels.add(vertex.getLabel());
      }

      addLabelFrequency(vertexLabelFrequency, vertexLabels);

    }
    cacheClient.addAndGetCounter(GRAPH_COUNT, graphCount);
    cacheClient.incrementAndGetCounter(GRAPH_COUNT_REPORTS);

    cacheClient
      .getList(LABEL_FREQUENCIES).add(vertexLabelFrequency);

    return cacheClient
      .incrementAndGetCounter(LABEL_FREQUENCY_REPORTS);
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

  private void setLocalFields() {
    cacheClient = DistributedCache.getClient(fsmConfig.getCacheServerAddress());
    graphs = Lists.newArrayList();
    partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    partition = getRuntimeContext().getIndexOfThisSubtask();
  }

}

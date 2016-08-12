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
import org.gradoop.flink.algorithms.fsm.gspan.encoders.tuples
  .EdgeTripleWithStringEdgeLabel;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFEdge;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EncodeWithCache extends
  RichMapPartitionFunction<TLFGraph, GSpanGraph> {

  public static final String VERTEX_PREFIX = "v";
  public static final String EDGE_PREFIX = "e";

  public static final String LABEL_FREQUENCIES = "lfs";
  public static final String LABEL_FREQUENCY_REPORTS = "vlfc";
  public static final String LABEL_DICTIONARY = "vld";
  public static final String LABEL_DICTIONARY_INVERSE = "vldi";
  public static final String LABEL_DICTIONARY_AVAILABLE = "vlda";

  private final FSMConfig fsmConfig;
  private Collection<TLFGraph> graphs;
  private int partitionCount;
  private int partition;
  private DistributedCacheClient cacheClient;

  public EncodeWithCache(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(Iterable<TLFGraph> values,
    Collector<GSpanGraph> out) throws Exception {

    setLocalFields();
    long vertexLabelReporter = reportVertexLabelFrequency(values);

    Map<String, Integer> vertexLabelDictionary =
      vertexLabelReporter == 1 ?
        createLabelDictionary(VERTEX_PREFIX) :
        readLabelDictionary(VERTEX_PREFIX);

    Collection<Collection<EdgeTripleWithStringEdgeLabel<Integer>>>
      graphTriples =  Lists.newArrayList();

    Map<String, Integer> labelFrequencies = Maps.newHashMap();

    for (TLFGraph graph : graphs) {
      Collection<EdgeTripleWithStringEdgeLabel<Integer>> triples = Lists
        .newArrayList();
      Set<String> edgeLabels = Sets.newHashSet();

      Map<Integer, Integer> vertexLabels = Maps.newHashMapWithExpectedSize
        (graph.getGraphVertices().size());

      for (TLFVertex vertex : graph.getGraphVertices()) {
        Integer label = vertexLabelDictionary.get(vertex.getLabel());
        if (label != null) {
          vertexLabels.put(vertex.getId(), label);
        }
      }

      for (TLFEdge edge : graph.getGraphEdges()) {
        Integer sourceId = edge.getSourceId();
        Integer sourceLabel = vertexLabels.get(sourceId);

        if (sourceLabel != null) {
          Integer targetId = edge.getTargetId();
          Integer targetLabel = vertexLabels.get(targetId);

          if (targetLabel != null) {
            String edgeLabel = edge.getLabel();
            edgeLabels.add(edgeLabel);
            triples.add(new EdgeTripleWithStringEdgeLabel<>(
              sourceId, targetId, edgeLabel, sourceLabel, targetLabel));
          }
        }
      }
      addLabelFrequency(labelFrequencies, edgeLabels);
    }

    cacheClient.getList(LABEL_FREQUENCIES).add(labelFrequencies);

    long reporter = cacheClient
      .incrementAndGetCounter(LABEL_FREQUENCY_REPORTS);

    Map<String, Integer> edgeLabelDictionary =
      reporter == 1 ?
        createLabelDictionary(EDGE_PREFIX) :
        readLabelDictionary(EDGE_PREFIX);

    System.out.println("HERE");
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

      dictionary.put(label, translation);
      inverseDictionary.add(label);

      translation++;
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

    for (TLFGraph graph : values) {
      graphs.add(graph);
      vertexLabels.clear();

      for (TLFVertex vertex : graph.getGraphVertices()) {
        vertexLabels.add(vertex.getLabel());
      }

      addLabelFrequency(vertexLabelFrequency, vertexLabels);

    }
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

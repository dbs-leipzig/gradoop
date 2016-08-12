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
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFGraph;
import org.gradoop.flink.io.impl.tlf.tuples.TLFVertex;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EncodeWithCache extends
  RichMapPartitionFunction<TLFGraph, GSpanGraph> {

  public static final String VERTEX_LABEL_FREQUENCIES = "vlf";
  public static final String VERTEX_LABEL_FREQUENCY_REPORTS = "vlfc";
  public static final String VERTEX_LABEL_DICTIONARY_AVAILABLE = "vlda";
  public static final String VERTEX_LABEL_DICTIONARY = "vld";
  public static final String VERTEX_LABEL_DICTIONARY_INVERSE = "vldi";
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
        createVertexLabelDictionary() :
        readVertexLabelDictionary();


    System.out.println("HERE");
  }

  private Map<String, Integer> readVertexLabelDictionary() throws
    InterruptedException {
    cacheClient.waitForEvent(VERTEX_LABEL_DICTIONARY_AVAILABLE);
    return cacheClient.getMap(VERTEX_LABEL_DICTIONARY);
  }

  private Map<String, Integer> createVertexLabelDictionary() throws
    InterruptedException {
    cacheClient.waitForCounterToReach(VERTEX_LABEL_FREQUENCY_REPORTS, partitionCount);

    Map<String, Integer> labelFrequencies = Maps.newHashMap();

    Collection<Map<String, Integer>> frequencyReports = cacheClient
      .getList(VERTEX_LABEL_FREQUENCIES);

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

    cacheClient.setMap(VERTEX_LABEL_DICTIONARY, dictionary);
    cacheClient.triggerEvent(VERTEX_LABEL_DICTIONARY_AVAILABLE);
    cacheClient.setList(VERTEX_LABEL_DICTIONARY_INVERSE, inverseDictionary);
    cacheClient.delete(VERTEX_LABEL_FREQUENCY_REPORTS);

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

      for (String vertexLabel : vertexLabels) {
        Integer frequency = vertexLabelFrequency.get(vertexLabel);

        if (frequency == null) {
          frequency = 1;
        } else {
          frequency += 1;
        }

        vertexLabelFrequency.put(vertexLabel, frequency);
      }

    }
    cacheClient
      .getList(VERTEX_LABEL_FREQUENCIES).add(vertexLabelFrequency);

    return cacheClient
      .incrementAndGetCounter(VERTEX_LABEL_FREQUENCY_REPORTS);
  }

  private void setLocalFields() {
    cacheClient = DistributedCache.getClient(fsmConfig.getCacheServerAddress());
    graphs = Lists.newArrayList();
    partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    partition = getRuntimeContext().getIndexOfThisSubtask();
  }

}

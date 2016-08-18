package org.gradoop.flink.algorithms.fsm.gspan.encoders.functions;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichMapPartitionFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.cache.DistributedCache;
import org.gradoop.common.cache.api.DistributedCacheClient;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;
import org.gradoop.common.model.impl.pojo.Vertex;
import org.gradoop.flink.algorithms.fsm.config.Constants;
import org.gradoop.flink.algorithms.fsm.config.FSMConfig;
import org.gradoop.flink.algorithms.fsm.gspan.encoders.comparators
  .LabelFrequencyEntryComparator;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyList;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.AdjacencyListEntry;
import org.gradoop.flink.algorithms.fsm.gspan.pojos.GSpanGraph;
import org.gradoop.flink.model.impl.tuples.GraphTransaction;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class EncodeGraphTransactionsWithCache
  extends RichMapPartitionFunction<GraphTransaction, GSpanGraph> {

  private final FSMConfig fsmConfig;
  private Collection<GraphTransaction> graphs;
  private Collection<Map<GradoopId, Integer>> graphVertexIdMap;
  private Collection<Map<Integer, AdjacencyList>> graphAdjacencyLists;


  private int partition;
  private int partitionCount;
  private DistributedCacheClient cacheClient;

  private long minFrequency;
  private Map<String, Integer> vertexLabelDictionary;
  private Map<String, Integer> edgeLabelDictionary;

  public EncodeGraphTransactionsWithCache(FSMConfig fsmConfig) {
    this.fsmConfig = fsmConfig;
  }

  @Override
  public void mapPartition(
    Iterable<GraphTransaction> values, Collector<GSpanGraph> out) throws Exception {

    graphs = Lists.newArrayList();
    partitionCount = getRuntimeContext().getNumberOfParallelSubtasks();
    partition = getRuntimeContext().getIndexOfThisSubtask();
    cacheClient = DistributedCache.getClient(fsmConfig.getCacheServerAddress());

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
  private long reportVertexLabelFrequency(Iterable<GraphTransaction> graphs) throws
    InterruptedException {

    Map<String, Integer> labelFrequency = Maps.newHashMap();

    long graphCount = 0;

    for (GraphTransaction graph : graphs) {
      graphCount++;
      this.graphs.add(graph);

      Set<String> vertexLabels = Sets.newHashSet();

      for (Vertex vertex : graph.getVertices()) {
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

  private long reportEdgeLabels() {
    Map<String, Integer> labelFrequency = Maps.newHashMap();

    graphAdjacencyLists = Lists.newArrayListWithCapacity(graphs.size());
    graphVertexIdMap = Lists.newArrayListWithCapacity(graphs.size());

    for (GraphTransaction graph : graphs) {
      Map<Integer, AdjacencyList> vertexAdjacencyLists = Maps.newHashMap();
      Map<GradoopId, Integer> vertexIdMap = Maps.newHashMap();
      Collection<String> edgeLabels = Sets.newHashSet();

      graphAdjacencyLists.add(vertexAdjacencyLists);
      graphVertexIdMap.add(vertexIdMap);

      // drop vertices with infrequent labels
      Iterator<Vertex> vertexIterator = graph.getVertices().iterator();

      int vertexId = 0;
      while (vertexIterator.hasNext()) {
        Vertex vertex = vertexIterator.next();

        Integer vertexLabel = vertexLabelDictionary.get(vertex.getLabel());

        if (vertexLabel != null) {
          vertexIdMap.put(vertex.getId(), vertexId);
          vertexAdjacencyLists.put(vertexId, new AdjacencyList(vertexLabel));
          vertexId++;
        } else {
          vertexIterator.remove();
        }
      }

      // drop edges with dropped vertices and report edge labels

      Iterator<Edge> edgeIterator = graph.getEdges().iterator();

      while (edgeIterator.hasNext()) {
        Edge edge = edgeIterator.next();

        if (vertexIdMap.containsKey(edge.getSourceId()) &&
          vertexIdMap.containsKey(edge.getTargetId())) {
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

  private Map<String, Integer> readLabelDictionary(String prefix) throws
    InterruptedException {
    cacheClient.waitForEvent(prefix + Constants.LABEL_DICTIONARY_AVAILABLE);
    return cacheClient.getMap(prefix + Constants.LABEL_DICTIONARY);
  }

  private void encodeGraphs() {

    Iterator<GraphTransaction> graphIterator = graphs.iterator();
    Iterator<Map<GradoopId, Integer>> vertexIdMapIterator =
      graphVertexIdMap.iterator();
    Iterator<Map<Integer, AdjacencyList>> adjacencyListsIterator =
      graphAdjacencyLists.iterator();

    while (graphIterator.hasNext()) {
      GraphTransaction graph = graphIterator.next();
      Map<GradoopId, Integer> vertexIdMap = vertexIdMapIterator.next();
      Map<Integer, AdjacencyList> adjacencyLists =
        adjacencyListsIterator.next();

      int edgeId = 0;

      for (Edge edge : graph.getEdges()) {
        Integer edgeLabel = edgeLabelDictionary.get(edge.getLabel());

        if (edgeLabel != null) {
          Integer sourceId = vertexIdMap.get(edge.getSourceId());
          AdjacencyList sourceAdjacencyList = adjacencyLists.get(sourceId);
          Integer sourceLabel = sourceAdjacencyList.getFromVertexLabel();

          Integer targetId = vertexIdMap.get(edge.getTargetId());
          AdjacencyList targetAdjacencyList = adjacencyLists.get(targetId);
          Integer targetLabel = targetAdjacencyList.getFromVertexLabel();

          sourceAdjacencyList.getEntries().add(new AdjacencyListEntry(
            true, edgeId, edgeLabel, targetId, targetLabel));

          targetAdjacencyList.getEntries().add(new AdjacencyListEntry(
            !fsmConfig.isDirected(), edgeId, edgeLabel, sourceId, sourceLabel));

          edgeId ++;
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

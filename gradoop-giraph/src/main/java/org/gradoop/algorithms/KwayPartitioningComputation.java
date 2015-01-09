package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.log4j.Logger;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;

/**
 * TODO: algorithm description
 */
public class KwayPartitioningComputation extends
  BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable,
    IntWritable> {

  private static final Logger LOG = Logger.getLogger
    (KwayPartitioningComputation.class);

  public final static String NUMBER_OF_PARTITIONS =
    "partitioning.num" + ".partitions";
  public final static String NUMBER_OF_ITERATIONS = "iterations";
  public static String DEFAULT_PARTITIONS = "2";
  public static final String KWAY_AGGREGATOR_CLASS =
    KwayPartitioningComputation.class.getName() + ".aggregator.class";
  public static final String KWAY_CAPACITY_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".capacity.aggregator.";
  public static final String KWAY_DEMAND_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".demand.aggregator.";

  private static final IntWritable ONE = new IntWritable(1);

  private int k;


  private int getDesiredPartition(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
    Iterable<IntWritable> messages) {

    int currentPartition = vertex.getValue().getCurrentVertexValue().get();
    int desiredPartition = currentPartition;


    LOG.info("=== vertex id: " + vertex.getId());
    LOG.info("=== current Partition: " + currentPartition);

    // got messages?
    if (messages.iterator().hasNext()) {

      // partition -> neighbours in partition
      int[] countNeighbours = getCountNeighbours(messages);
      LOG.info("=== count neighbours:");
      for (int i = 0; i < countNeighbours.length; i++) {
        LOG.info(String.format("%d => %d", i, countNeighbours[i]));
      }

      // partition -> desire to migrate
      double[] partitionWeights =
        getPartitionWeight(countNeighbours, vertex.getNumEdges());
      LOG.info("=== partition weights:");
      for (int i = 0; i < partitionWeights.length; i++) {
        LOG.info(String.format("%d => %f.2", i, partitionWeights[i]));
      }


      double firstMax = Integer.MIN_VALUE;
      double secondMax = Integer.MIN_VALUE;
      int firstK = -1;
      int secondK = -1;
      for (int i = 0; i < k; i++) {
        if (partitionWeights[i] >= firstMax) {
          firstMax = partitionWeights[i];
          firstK = i;
        } else if (partitionWeights[i] > secondMax) {
          secondMax = partitionWeights[i];
          secondK = i;
        }

      }

      LOG.info("=== firstMax: " + firstMax);
      LOG.info("=== secondMax: " + secondMax);
      LOG.info("=== firstK: " + firstK);
      LOG.info("=== secondK: " + secondK);

      if (firstMax == secondMax) {
        if (currentPartition != firstK && currentPartition != secondK) {
          desiredPartition = firstK;
        }
      } else {
        desiredPartition = firstK;
      }
    }
    LOG.info("=== switching to: " + desiredPartition);
    return desiredPartition;
  }

  private int[] getCountNeighbours(Iterable<IntWritable> messages) {
    int[] countNeighbours = new int[k];

    for (IntWritable message : messages) {
      int partition = message.get();
      countNeighbours[partition]++;
    }
    return countNeighbours;
  }

  private double[] getPartitionWeight(int[] partitionCount, int numEdges) {
    double[] partitionWeights = new double[k];
    for (int i = 0; i < k; i++) {
      String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX + i;
      IntWritable aggregator_load = getAggregatedValue(aggregator);
      double load = aggregator_load.get();
      double numNeighboursInI = partitionCount[i];
      double weight = numNeighboursInI / (load * numEdges);
      partitionWeights[i] = weight;
    }
    return partitionWeights;
  }

  private void notifyDemandAggregator(int desiredPartition) {
    String aggregator = KWAY_DEMAND_AGGREGATOR_PREFIX + desiredPartition;
    aggregate(aggregator, ONE);
  }

  private boolean doMigrate(int desiredPartition) {
    String capacity_aggregator =
      KWAY_CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    String demand_aggregator = KWAY_DEMAND_AGGREGATOR_PREFIX + desiredPartition;
    double partitionCount = Double.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    double total_cpacity = getTotalNumVertices() / partitionCount +
      (getTotalNumVertices() / partitionCount * 0.5);
    IntWritable load_capacity = getAggregatedValue(capacity_aggregator);
    double load = load_capacity.get();
    double availability = total_cpacity - load;
    IntWritable demand_value = getAggregatedValue(demand_aggregator);
    double demand = demand_value.get();
    double threshold = availability / demand;
    double randomRange = Math.random();
    return Double.compare(randomRange, threshold) < 0;
  }

  private void migrateVertex(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
    int desiredPartition) {
    String oldPartition = KWAY_CAPACITY_AGGREGATOR_PREFIX +
      vertex.getValue().getCurrentVertexValue().get();
    String newPartition = KWAY_CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    notifyCapacityAggregator(oldPartition, -1);
    vertex.getValue().setCurrentVertexValue(new IntWritable(desiredPartition));
    notifyCapacityAggregator(newPartition, 1);
  }

  private void setVertexStartValue(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex) {
    int startValue = vertex.getId().get() % k;
    vertex.getValue().setCurrentVertexValue(new IntWritable(startValue));
  }

  private void notifyCapacityAggregator(String aggregator, int x) {
    aggregate(aggregator, new IntWritable(x));
  }

  /**
   * The actual KwayPartitioning Computation
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.
   * @throws IOException
   */
  @Override
  public void compute(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
    Iterable<IntWritable> messages)
    throws IOException {
    LOG.info("=== superstep: " + getSuperstep());
    k = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    if (getSuperstep() == 0) {
      setVertexStartValue(vertex);
      String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX +
        vertex.getValue().getCurrentVertexValue().get();
      notifyCapacityAggregator(aggregator, 1);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentVertexValue());
    } else {
      // even superstep: migrate phase
      if ((getSuperstep() % 2) == 0) {
        int desiredPartition = vertex.getValue().getLastVertexValue().get();
        int currentPartition = vertex.getValue().getCurrentVertexValue().get();
        LOG.info("=== vertex id: " + vertex.getId());
        LOG.info("=== currentPartition: " + currentPartition);
        LOG.info("=== desiredPartition: " + desiredPartition);
        if (desiredPartition != currentPartition) {
          boolean migrate = doMigrate(desiredPartition);
          LOG.info("=== doMigrate: " + migrate);
          if (migrate) {
            migrateVertex(vertex, desiredPartition);
            sendMessageToAllEdges(vertex,
              vertex.getValue().getCurrentVertexValue());
          }
        }
        vertex.voteToHalt();
      } else { // odd supersteps: demand phase
        int desiredPartition = getDesiredPartition(vertex, messages);
        vertex.getValue().setLastVertexValue(new IntWritable(desiredPartition));
        int currentValue = vertex.getValue().getCurrentVertexValue().get();
        boolean changed = currentValue != desiredPartition;
        if (changed) {
          notifyDemandAggregator(desiredPartition);
        }
      }
    }
  }
}

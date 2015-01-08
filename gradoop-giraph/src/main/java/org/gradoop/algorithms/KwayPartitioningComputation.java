package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * TODO: algorithm description
 */
public class KwayPartitioningComputation extends
  BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable,
    IntWritable> {
  public final static String NUMBER_OF_PARTITIONS =
    "partitioning.num" + ".partitions";
  public static String DEFAULT_PARTITIONS = "2";
  public static final String KWAY_AGGREGATOR_CLASS =
    KwayPartitioningComputation.class.getName() + ".aggregator.class";
  public static final String KWAY_CAPACITY_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".capacity.aggregator.";
  public static final String KWAY_DEMAND_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".demand.aggregator.";

  private int getHighestWeight(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex,
    Iterable<IntWritable> messages) {

    int desiredPartition = 0;

    Map<Integer, Integer> countNeighbours = new HashMap<>();
    Map<Integer, Double> partitionWeight = new HashMap<>();

    for (IntWritable message : messages) {
      if (!countNeighbours.containsKey(message.get())) {
        countNeighbours.put(message.get(), 1);
      } else {
        countNeighbours
          .put(message.get(), countNeighbours.get(message.get()) + 1);
      }
    }
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    int totalNeighbours = countNeighbours.size();
    for (int i = 0; i < partitionCount; i++) {
      String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX + i;
      int load = getAggregatedValue(aggregator);
      int numNeighboursInI = countNeighbours.get(i);
      double weight = (1 / load) * numNeighboursInI / totalNeighbours;
      partitionWeight.put(i, weight);
    }
    double highestWeight = 0;
    double secondHighestWeight = 0;
    int secondKey = 0;
    for (Map.Entry<Integer, Double> entry : partitionWeight.entrySet()) {
      if (highestWeight < entry.getValue()) {
        secondHighestWeight = highestWeight;
        secondKey = desiredPartition;
        desiredPartition = entry.getKey();
        highestWeight = entry.getValue();
      }
    }
    if (secondHighestWeight == highestWeight) {
      if (vertex.getValue().getCurrentVertexValue().get() == desiredPartition
        || vertex.getValue().getCurrentVertexValue().get() == secondKey) {
        desiredPartition = vertex.getValue().getCurrentVertexValue().get();
      }
    }
    return desiredPartition;
  }

  private void notifyDemandAggregator(int desiredPartition) {
    String aggregator = KWAY_DEMAND_AGGREGATOR_PREFIX + desiredPartition;
    aggregate(aggregator, new IntWritable(1));
  }

  private boolean calculateThreshold(int desiredPartition) {
    String capacity_aggregator =
      KWAY_CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    String demand_aggregator = KWAY_DEMAND_AGGREGATOR_PREFIX + desiredPartition;
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    double total_cpacity = getTotalNumVertices() / partitionCount +
      (getTotalNumVertices() / partitionCount * 0.2);
    int load = getAggregatedValue(capacity_aggregator);
    double availability = total_cpacity - load;
    int demand = getAggregatedValue(demand_aggregator);
    double treshhold = availability / demand;
    double randomRange = Math.random();
    return randomRange < treshhold;
  }

  private void migrateVertex(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex, int desiredPartition) {
    String oldPartition = KWAY_CAPACITY_AGGREGATOR_PREFIX + vertex.getValue()
      .getCurrentVertexValue().get();
    String newPartition = KWAY_CAPACITY_AGGREGATOR_PREFIX + desiredPartition;
    notifyCapacityAggregator(oldPartition, -1);
    vertex.getValue().setCurrentVertexValue(new IntWritable(desiredPartition));
    notifyCapacityAggregator(newPartition, 1);
  }

  private void setVertexStartValue(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex) {
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    int startValue = vertex.getId().get() % partitionCount;
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
    Iterable<IntWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      setVertexStartValue(vertex);
      String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX + vertex.getValue()
        .getCurrentVertexValue().get();
      notifyCapacityAggregator(aggregator, 1);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentVertexValue());
      vertex.voteToHalt();
    } else {

      if ((getSuperstep() % 2) == 0) {
        int desiredPartition = vertex.getValue().getLastVertexValue().get();
        boolean migrate =
          calculateThreshold(desiredPartition);
        if (migrate) {
          migrateVertex(vertex, desiredPartition);
          sendMessageToAllEdges(vertex,
            vertex.getValue().getCurrentVertexValue());
        } else {
          vertex.voteToHalt();
        }


      } else if ((getSuperstep() % 2) == 1) {
        int desiredPartition = getHighestWeight(vertex, messages);
        vertex.getValue().setLastVertexValue(new IntWritable(desiredPartition));
        int currentValue = vertex.getValue().getCurrentVertexValue().get();
        boolean changed = currentValue != desiredPartition;
        if (changed) {
          notifyDemandAggregator(desiredPartition);
        } else {
          vertex.voteToHalt();
        }
      }
    }
    vertex.voteToHalt();
  }
}

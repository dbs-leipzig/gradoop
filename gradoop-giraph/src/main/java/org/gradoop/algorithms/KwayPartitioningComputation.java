package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.commons.lang.math.RandomUtils.nextInt;

/**
 * TODO: algorithm description
 */
public class KwayPartitioningComputation extends
  BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable,
    IntWritable> {


  public final static String NUMBER_OF_PARTITIONS =
    "partitioning.num" + ".partitions";
  public static String COMPUTATION_CASE = "partitioning.case";
  public static String DEFAULT_PARTITIONS = "2";
  public static final String KWAY_AGGREGATOR_CLASS =
    KwayPartitioningComputation.class.getName() + ".aggregator.class";
  public static final String KWAY_CAPACITY_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".capacity.aggregator.";
  public static final String KWAY_DEMAND_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".demand.aggregator.";


  private int getHighestWeight(Iterable<IntWritable> messages) {

    int desiredPartition=0;

    Map<Integer, Integer> countNeighbours = new HashMap<>();
    Map<Integer, Float> partitionWeight = new HashMap<>();

    for (IntWritable message : messages) {
      if(!countNeighbours.containsKey(message.get())){
        countNeighbours.put(message.get(),1);
      }else{
        countNeighbours.put(message.get(),countNeighbours.get(message.get())+1);
      }
    }

    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));

    for(int i=0; i<partitionCount;i++){
      String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX + i;
      int load = getAggregatedValue(aggregator);
      int totalNeighbours = countNeighbours.size();
      int numNeighboursInI = countNeighbours.get(i);
      float weight = (1/load) * numNeighboursInI / totalNeighbours;
      partitionWeight.put(i,weight);
    }

    for(Map.Entry<Integer,Float> entry : partitionWeight.entrySet()){

    }





    return desiredPartition;
  }

  /**
   * Returns the most frequent value based on all received messages.
   *
   * @param vertex      The current vertex
   * @param allMessages All messages the current vertex has received
   * @return the maximal frequent number in all received messages
   */
  private int getMostFrequent(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
    List<Integer> allMessages, String computation_case) {
    Collections.sort(allMessages);
    int newValue;
    int currentCounter = 1;
    int currentValue = allMessages.get(0);
    int maxCounter = 1;
    int maxValue = 1;
    for (int i = 1; i < allMessages.size(); i++) {
      if (currentValue == allMessages.get(i)) {
        currentCounter++;
        if (maxCounter < currentCounter) {
          maxCounter = currentCounter;
          maxValue = currentValue;
        }
      } else {
        currentCounter = 1;
        currentValue = allMessages.get(i);
      }
    }
    // if the frequent of all received messages are one
    if (maxCounter == 1) {
      switch (computation_case) {
      case "min":
        newValue = getSwitchValue(allMessages.get(0), vertex, computation_case);
        break;
      case "max":
        newValue =
          getSwitchValue(allMessages.get(allMessages.size() - 1), vertex,
            computation_case);
        break;
      case "rdm":
        newValue =
          getSwitchValue(allMessages.get(nextInt(allMessages.size() - 1)),
            vertex, computation_case);
        break;
      default:
        newValue = getSwitchValue(allMessages.get(0), vertex, computation_case);
        break;
      }
    } else {
      newValue = getSwitchValue(maxValue, vertex, computation_case);
    }
    return newValue;
  }

  /**
   * Decides how the computation should work in different cases.
   *
   * @param value1 int value to compare with
   * @param vertex The current vertex
   * @return new vertex value
   */
  private int getSwitchValue(int value1,
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex,
    String computation_case) {
    int value;
    switch (computation_case) {
    case "min":
      value = Math.min(value1, vertex.getValue().getLastVertexValue().get());
      break;
    case "max":
      value = Math.max(value1, vertex.getValue().getCurrentVertexValue().get());
      break;
    case "rdm":
      if (value1 == vertex.getValue().getLastVertexValue().get()) {
        value = vertex.getValue().getCurrentVertexValue().get();
      } else {
        value = value1;
      }
      break;
    default:
      value = Math.min(value1, vertex.getValue().getCurrentVertexValue().get());
    }
    //}
    return value;
  }

  private void setVertexStartValue(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex) {
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    int startValue = vertex.getId().get() % partitionCount;
    vertex.getValue().setCurrentVertexValue(new IntWritable(startValue));
    vertex.getValue()
      .setLastVertexValue(vertex.getValue().getCurrentVertexValue());
  }

  private void notifyAggregator(
    Vertex<IntWritable, KwayPartitioningVertex, NullWritable> vertex, int x) {
    String aggregator = KWAY_CAPACITY_AGGREGATOR_PREFIX +
      vertex.getValue().getCurrentVertexValue().get();
    aggregate(aggregator, new IntWritable(x));
  }

  private boolean checkAggregatorSpace(int newValue, int sum) {
    String aggregator = KWAY_AGGREGATOR_PREFIX + newValue;
    IntWritable aggregatedSum = getAggregatedValue(aggregator);
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));

    return ((aggregatedSum.get() + 1) < sum / partitionCount);
  }

  private int getNodeCountAndReset() {
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    int sum = 0;
    IntWritable aggregatedValue;
    for (int i = 0; i < partitionCount; i++) {
      aggregatedValue = getAggregatedValue(KWAY_AGGREGATOR_PREFIX + i);
      sum += aggregatedValue.get();
      int x = aggregatedValue.get();
      aggregate(KWAY_AGGREGATOR_PREFIX + i, new IntWritable(-x));
    }
    return sum;
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

    String computation_case = String.valueOf(getConf().get(COMPUTATION_CASE));
    int nodecount = 0;

    if (getSuperstep() == 0) {
      setVertexStartValue(vertex);
      notifyAggregator(vertex, 1);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentVertexValue());
      vertex.voteToHalt();
    } else {
      if((getSuperstep()%2) == 0){


      }else{
        int desiredPartition = getHighestWeight(messages);


      }
      int currentMinValue = vertex.getValue().getCurrentVertexValue().get();
      int newValue = getNewValue(vertex, messages, computation_case);
      boolean changed = currentMinValue != newValue;
      if (changed) {
        if (checkAggregatorSpace(newValue, nodecount)) {
          vertex.getValue()
            .setLastVertexValue(new IntWritable(currentMinValue));
          notifyAggregator(vertex, -1);

          vertex.getValue().setCurrentVertexValue(new IntWritable(newValue));
          notifyAggregator(vertex, 1);
          sendMessageToAllEdges(vertex,
            vertex.getValue().getCurrentVertexValue());
        } else {
          notifyAggregator(vertex, +1);
          vertex.voteToHalt();
        }
      } else {
        vertex.voteToHalt();
      }
    }
    vertex.voteToHalt();
  }
}

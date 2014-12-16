package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.io.KwayPartitioningVertex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.apache.commons.lang.math.RandomUtils.nextInt;

/**
 * TODO: algorithm description
 */
public class KwayPartitioningComputation extends
  BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable,
    IntWritable> {
  public final static String NUMBER_OF_PARTITIONS = "partitioning.num" +
    ".partitions";
  public static String COMPUTATION_CASE = "partitioning.case";
  public static String DEFAULT_PARTITIONS = "2";
  public static final String KWAY_AGGREGATOR_CLASS =
    KwayPartitioningComputation.class.getName() + ".aggregator.class";
  public static final String KWAY_AGGREGATOR_PREFIX =
    KwayPartitioningComputation.class.getName() + ".kway.aggregator.";

  /**
   * Returns the current new value. This value is based on all incoming
   * messages. Depending on the number of messages sent to the vertex, the
   * method returns:static
   * <p/>
   * 0 messages:   The current value
   * <p/>
   * 1 message:    The minimum of the message and the current vertex value
   * <p/>
   * >1 messages:  The most frequent of all message values
   *
   * @param vertex   The current vertex
   * @param messages All incoming messages
   * @return the new Value the vertex will become
   */
  private int getNewValue(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex, Iterable<IntWritable> messages, String computation_case) {
    int newValue;
    //TODO: create allMessages more efficient
    //List<IntWritable> allMessages = Lists.newArrayList(messages);
    List<Integer> allMessages = new ArrayList<>();
    for (IntWritable message : messages) {
      allMessages.add(message.get());
    }
    if (allMessages.isEmpty()) {
      // 1. if no messages are received
      newValue = vertex.getValue().getCurrentVertexValue().get();
    } else if (allMessages.size() == 1) {
      newValue = getSwitchValue(allMessages.get(0), vertex, computation_case);
      // 2. if just one message are received
    } else {
      // 3. if multiple messages are received
      newValue = getMostFrequent(vertex, allMessages, computation_case);
    }
    return newValue;
  }

  /**
   * Returns the most frequent value based on all received messages.
   *
   * @param vertex      The current vertex
   * @param allMessages All messages the current vertex has received
   * @return the maximal frequent number in all received messages
   */
  private int getMostFrequent(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex, List<Integer> allMessages, String computation_case) {
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
          newValue = getSwitchValue(allMessages.get(allMessages.size() - 1),
            vertex, computation_case);
          break;
        case "rdm":
          newValue = getSwitchValue(allMessages.get(nextInt
            (allMessages.size() - 1)), vertex, computation_case);
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
                             Vertex<IntWritable, KwayPartitioningVertex,
                               NullWritable> vertex, String computation_case) {
    int value;
    switch (computation_case) {
      case "min":
        value =
          Math.min(value1, vertex.getValue().getLastVertexValue().get());
        break;
      case "max":
        value =
          Math.max(value1, vertex.getValue().getCurrentVertexValue().get());
        break;
      case "rdm":
        if (value1 == vertex.getValue().getLastVertexValue().get()) {
          value = vertex.getValue().getCurrentVertexValue().get();
        } else {
          value = value1;
        }
        break;
      default:
        value =
          Math.min(value1, vertex.getValue().getCurrentVertexValue().get());
    }
    //}
    return value;
  }

  private void setVertexStartValue(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex) {
    int partitionCount = Integer.valueOf(getConf().get(NUMBER_OF_PARTITIONS));
    vertex.getValue().setCurrentVertexValue(new IntWritable
      (nextInt(partitionCount)));
    vertex.getValue()
      .setLastVertexValue(
        vertex.getValue().getCurrentVertexValue());
  }

  private void notifyAggregator(Vertex<IntWritable, KwayPartitioningVertex,
    NullWritable> vertex) {
    String aggregator = KWAY_AGGREGATOR_PREFIX + vertex.getValue()
      .getCurrentVertexValue().get();
    aggregate(aggregator, new IntWritable(1));
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

    String computation_case = String.valueOf(getConf().get
      (COMPUTATION_CASE));

    if (getSuperstep() == 0) {
      setVertexStartValue(vertex);
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentVertexValue());
      vertex.voteToHalt();
    } else {
      int currentMinValue = vertex.getValue().getCurrentVertexValue().get();
      int newValue = getNewValue(vertex, messages, computation_case);
      boolean changed = currentMinValue != newValue;
      if (changed) {
        vertex.getValue().setLastVertexValue(new IntWritable(currentMinValue));
        vertex.getValue().setCurrentVertexValue(new IntWritable(newValue));
        sendMessageToAllEdges(vertex,
          vertex.getValue().getCurrentVertexValue());
      } else {
        vertex.voteToHalt();
        notifyAggregator(vertex);
      }
    }
    vertex.voteToHalt();
    notifyAggregator(vertex);
  }
}

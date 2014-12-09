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
import java.util.Random;

/**
 * TODO: algorithm description
 */
public class KwayPartitioningComputation extends
  BasicComputation<IntWritable, KwayPartitioningVertex, NullWritable,
    IntWritable> {

  private final static int number_of_partitions = 2;

  private final static String computation_case = "rdm";

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
    NullWritable> vertex, Iterable<IntWritable> messages) {
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
      newValue = getSwitchValue(allMessages.get(0), vertex);
      // 2. if just one message are received
    } else {
      // 3. if multiple messages are received
      newValue = getMostFrequent(vertex, allMessages);
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
    NullWritable> vertex, List<Integer> allMessages) {
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
          newValue = getSwitchValue(allMessages.get(0), vertex);
          break;
        case "max":
          newValue = getSwitchValue(allMessages.get(allMessages.size() - 1),
            vertex);
          break;
        case "rdm":
          newValue = getSwitchValue(allMessages.get(new Random().nextInt
            (allMessages.size() - 1)), vertex);
          break;
        default:
          newValue = getSwitchValue(allMessages.get(0), vertex);
      }
    } else {
      newValue = getSwitchValue(maxValue, vertex);
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
  public int getSwitchValue(int value1,
                            Vertex<IntWritable, KwayPartitioningVertex,
                              NullWritable> vertex) {
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
          value = vertex.getValue().getLastVertexValue().get();
        } else {

          value = Math.min(value1, vertex.getValue().getLastVertexValue().get
            ());


        }
        break;
      default:
        value =
          Math.min(value1, vertex.getValue().getCurrentVertexValue().get());
    }
    //}
    return value;
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
    if (getSuperstep() == 0) {
      Random randomGenerator = new Random();
      vertex.getValue().setCurrentVertexValue(new IntWritable
        (randomGenerator.nextInt(number_of_partitions)));
      vertex.getValue()
        .setLastVertexValue(vertex.getValue().getCurrentVertexValue());
      sendMessageToAllEdges(vertex, vertex.getValue().getCurrentVertexValue());
      vertex.voteToHalt();
    } else {
      int currentMinValue = vertex.getValue().getCurrentVertexValue().get();
      int newValue = getNewValue(vertex, messages);
      boolean changed = currentMinValue != newValue;
      if (changed) {
        vertex.getValue().setLastVertexValue(new IntWritable(currentMinValue));
        vertex.getValue().setCurrentVertexValue(new IntWritable(newValue));
        sendMessageToAllEdges(vertex,
          vertex.getValue().getCurrentVertexValue());
      } else {
        vertex.voteToHalt();
      }
    }
    vertex.voteToHalt();
  }
}

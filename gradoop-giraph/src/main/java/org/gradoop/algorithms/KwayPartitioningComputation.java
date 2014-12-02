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

  private final static int number_of_partitions = 10;

  /**
   * Returns the current new value. This value is based on all incoming
   * messages. Depending on the number of messages sent to the vertex, the
   * method returns:
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
      // 2. if just one message are received
      if(allMessages.get(0) == vertex.getValue().getLastVertexValue().get()){
        newValue = vertex.getValue().getCurrentVertexValue().get();
      }else{
        newValue = allMessages.get(0);
      }
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
      newValue = allMessages.get(new Random().nextInt(allMessages.size()));
    } else {
      if (maxValue == vertex.getValue().getLastVertexValue().get()) {
        newValue = vertex.getValue().getCurrentVertexValue().get();
      } else {
        newValue = maxValue;
      }
    }
    return newValue;
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
      vertex.getValue().setLastVertexValue(new IntWritable(Integer.MAX_VALUE));
      Random randomGenerator = new Random();
      vertex.getValue().setCurrentVertexValue(new IntWritable
        (randomGenerator.nextInt
        (number_of_partitions)));
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

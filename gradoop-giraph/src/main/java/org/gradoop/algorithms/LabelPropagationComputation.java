package org.gradoop.algorithms;

import org.apache.giraph.comm.WorkerClientRequestProcessor;
import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.GraphState;
import org.apache.giraph.graph.GraphTaskManager;
import org.apache.giraph.graph.Vertex;
import org.apache.giraph.worker.WorkerContext;
import org.apache.giraph.worker.WorkerGlobalCommUsage;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Implementation of the Label Propagation Algorithm:
 * The input graph as adjacency list contains the information about the
 * vertex (id), value (label) and its edges to neighbors.
 * <p/>
 * In super step 0 each vertex will propagate its value within his neighbors
 * <p/>
 * In the remaining super steps each vertex will adopt the value of the
 * majority of their neighbors or the smallest one if there are just one
 * neighbor. If a vertex adopt a new value it'll propagate the new one again.
 * <p/>
 * The computation will terminate if no new values are assigned.
 */
public class LabelPropagationComputation extends
  BasicComputation<LongWritable, LongWritable, NullWritable, LongWritable> {
  /**
   * Number of migrations the Vertex can do
   */
  public static final String NUMBER_OF_ITERATIONS =
    "labelpropagation.numberofiterations";
  /**
   * Default number of migrations if no value is given
   */
  public static final int DEFAULT_NUMBER_OF_ITERATIONS = 10;

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
  private long getNewValue(
    Vertex<LongWritable, LongWritable, NullWritable> vertex,
    Iterable<LongWritable> messages) {
    long newValue;
    //TODO: create allMessages more efficient
    //List<LongWritable> allMessages = Lists.newArrayList(messages);
    List<Long> allMessages = new ArrayList<>();
    for (LongWritable message : messages) {
      allMessages.add(message.get());
    }
    if (allMessages.isEmpty()) {
      // 1. if no messages are received
      newValue = vertex.getValue().get();
    } else if (allMessages.size() == 1) {
      // 2. if just one message are received
      newValue = Math.min(vertex.getValue().get(), allMessages.get(0));
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
  private long getMostFrequent(
    Vertex<LongWritable, LongWritable, NullWritable> vertex,
    List<Long> allMessages) {
    Collections.sort(allMessages);
    long newValue;
    int currentCounter = 1;
    long currentValue = allMessages.get(0);
    int maxCounter = 1;
    long maxValue = 1;
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
    // if the frequent of all received messages are just one
    if (maxCounter == 1) {
      // to avoid an oscillating state of the calculation we will just use
      // the smaller value
      newValue = Math.min(vertex.getValue().get(), allMessages.get(0));
    } else {
      newValue = maxValue;
    }
    return newValue;
  }

  @Override
  public void initialize(GraphState graphState,
    WorkerClientRequestProcessor<LongWritable, LongWritable, NullWritable>
      workerClientRequestProcessor,
    GraphTaskManager<LongWritable, LongWritable, NullWritable> graphTaskManager,
    WorkerGlobalCommUsage workerGlobalCommUsage, WorkerContext workerContext) {
    super.initialize(graphState, workerClientRequestProcessor, graphTaskManager,
      workerGlobalCommUsage, workerContext);
  }

  /**
   * The actual LabelPropagation Computation
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.
   * @throws IOException
   */
  @Override
  public void compute(Vertex<LongWritable, LongWritable, NullWritable> vertex,
    Iterable<LongWritable> messages) throws IOException {
    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getId());
    } else {
      long currentMinValue = vertex.getValue().get();
      long newValue = getNewValue(vertex, messages);
      boolean changed = currentMinValue != newValue;
      if (changed) {
        vertex.setValue(new LongWritable(newValue));
        sendMessageToAllEdges(vertex, vertex.getValue());
      }
    }
    vertex.voteToHalt();
  }
}

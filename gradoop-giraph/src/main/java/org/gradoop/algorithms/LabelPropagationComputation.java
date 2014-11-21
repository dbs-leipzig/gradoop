/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by gomezk on 03.11.14.
 */
public class LabelPropagationComputation extends
  BasicComputation<IntWritable, IntWritable, NullWritable, IntWritable> {
  /**
   * Returns the current new value. This value si based on all incoming
   * messages. The method return: 1. The current value if no messages are
   * received 2. If just one message are received it will return the min of
   * received message or current vertex value 3. The maxFrequent number in all
   * received messages
   *
   * @param vertex   The current vertex
   * @param messages All incoming messages
   * @return the new Value the vertex will become
   */
  private int getNewValue(Vertex<IntWritable, IntWritable,
    NullWritable> vertex, Iterable<IntWritable> messages) {
    int newValue;
    //TODO: allMessages besser befuellen.
    //List<IntWritable> allMessages = Lists.newArrayList(messages);
    List<Integer> allMessages = new ArrayList<>();
    for (IntWritable message : messages) {
      allMessages.add(message.get());
    }
    if (allMessages.isEmpty()) {
      //1. if no messages are received
      newValue = vertex.getValue().get();
    } else if (allMessages.size() == 1) {
      //2. if just one message are received
      newValue = Math.min(vertex.getValue().get(), allMessages.get(0));
    } else {
      //3. if multiple messages are received
      newValue = getMaxFrequent(vertex, allMessages);
    }
    return newValue;
  }

  /**
   * Returns the maximal frequent number based on all received messages of the
   * current vertex
   *
   * @param vertex      The current vertex
   * @param allMessages All messages the current vertex has received
   * @return the maximal frequent number in all received messages
   */
  private int getMaxFrequent(Vertex<IntWritable, IntWritable,
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
    //if the frequent of all received messages are just one
    if (maxCounter == 1) {
      // to avoid an oscillating state of the calculation we will just use
      // the smaller value
      newValue = Math.min(vertex.getValue().get(), allMessages.get(0));
    } else {
      newValue = maxValue;
    }
    return newValue;
  }

  /**
   * The actual LabelPropagation Computation
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have a life
   * @throws IOException
   */
  @Override
  public void compute(Vertex<IntWritable, IntWritable, NullWritable> vertex,
                      Iterable<IntWritable> messages)
    throws IOException {
    if (getSuperstep() == 0) {
      sendMessageToAllEdges(vertex, vertex.getValue());
      vertex.voteToHalt();
    } else {
      int currentMinValue = vertex.getValue().get();
      int newValue = getNewValue(vertex, messages);
      boolean changed = currentMinValue != newValue;
      if (changed) {
        vertex.setValue(new IntWritable(newValue));
        sendMessageToAllEdges(vertex, vertex.getValue());
      } else {
        vertex.voteToHalt();
      }
    }
    vertex.voteToHalt();
  }
}

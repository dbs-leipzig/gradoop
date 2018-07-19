/*
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradoop.flink.algorithms.gelly.labelpropagation.functions;

import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.spargel.GatherFunction;
import org.apache.flink.graph.spargel.MessageIterator;
import org.apache.flink.hadoop.shaded.com.google.common.collect.Lists;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Collections;
import java.util.List;

/**
 * Updates the value of a vertex by picking the most frequent value out of
 * all incoming values.
 */
public class LPUpdateFunction
  extends GatherFunction<GradoopId, PropertyValue, PropertyValue> {
  /**
   * Updates the vertex value if it has changed.
   *
   * @param vertex  vertex to be updated
   * @param msg     message
   * @throws Exception
   */
  @Override
  public void updateVertex(Vertex<GradoopId, PropertyValue> vertex,
    MessageIterator<PropertyValue> msg) throws Exception {
    PropertyValue value = getNewValue(vertex,
      Lists.newArrayList(msg.iterator()));
    if (!vertex.getValue().equals(value)) {
      setNewVertexValue(value);
    }
  }

  /**
   * Returns the new value based on all incoming messages. Depending on the
   * number of messages sent to the vertex, the method returns:
   * <p/>
   * 0 messages:   The current value
   * <p/>
   * 1 message:    The minimum of the message and the current vertex value
   * <p/>
   * >1 messages:  The most frequent of all message values
   * <p/>
   * >1 messages, same frequency: The minimum of the most frequent labels
   *
   * @param vertex      the current vertex
   * @param allMessages all received messages
   * @return most frequent value below all messages
   */
  private PropertyValue getNewValue(Vertex<GradoopId, PropertyValue> vertex,
    List<PropertyValue> allMessages) {

    Collections.sort(allMessages);
    PropertyValue newValue;
    int currentCounter = 1;
    PropertyValue currentValue = allMessages.get(0);
    int maxCounter = 1;
    PropertyValue maxValue = PropertyValue.NULL_VALUE;
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
    // if each label has a frequency of one
    if (maxCounter == 1) {
      // to avoid an oscillating state of the calculation we will just use
      // the smaller value
      newValue = getMinimum(vertex.getValue(), allMessages.get(0));
    } else {
      newValue = maxValue;
    }
    return newValue;
  }

  /**
   * Compares two PropertyValues and returns the smaller one
   *
   * @param v1 PropertyValue 1
   * @param v2 PropertyValue 2
   * @return returns the smaller PropertyValue
   */
  private PropertyValue getMinimum(PropertyValue v1, PropertyValue v2) {
    PropertyValue newValue;
    int compare = v1.compareTo(v2);
    if (compare <= 0) {
      newValue = v1;
    } else {
      newValue = v2;
    }
    return newValue;
  }
}

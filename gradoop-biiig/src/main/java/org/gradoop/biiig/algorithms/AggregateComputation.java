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

package org.gradoop.biiig.algorithms;

import org.apache.giraph.graph.BasicComputation;
import org.apache.giraph.graph.Vertex;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.gradoop.biiig.io.formats.BTGVertexValue;

import java.io.IOException;

/**
 * Used for aggregate calculation of each BTG inside the graph. The aggregation
 * is based on a user-defined function.
 */
public class AggregateComputation extends BasicComputation<
  LongWritable, BTGVertexValue, NullWritable, NullWritable> {
  /**
   * Prefix for global aggregators for specific BTGs
   */
  public static final String BTG_AGGREGATOR_PREFIX =
    AggregateComputation.class.getName() + ".btg.aggregator.";
  /**
   * Configuration parameter to define the number of BTGs included in the input
   * graph.
   */
  public static final String BTG_AGGREGATOR_CNT =
    AggregateComputation.class.getName() + ".btg.count";
  /**
   * Configuration parameter to define the Aggregator class to be used in global
   * aggregation.
   */
  public static final String BTG_AGGREGATOR_CLASS =
    AggregateComputation.class.getName() + ".aggregator.class";

  /**
   * Default value for BTG_CNT.
   */
  public static final Long DEFAULT_BTG_CNT = 10L;

  @Override
  public void compute(
    Vertex<LongWritable, BTGVertexValue, NullWritable> vertex,
    Iterable<NullWritable> messages)
    throws IOException {
    if (getSuperstep() == 0) {
      for (long btgID : vertex.getValue().getGraphs()) {
        String aggregator = BTG_AGGREGATOR_PREFIX + btgID;
        aggregate(aggregator,
          new DoubleWritable(vertex.getValue().getVertexValue()));
      }
      vertex.voteToHalt();
    }
  }
}

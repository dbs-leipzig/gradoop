/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
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
public class AggregateComputation extends
  BasicComputation<LongWritable, BTGVertexValue, NullWritable, NullWritable> {
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
  public void compute(Vertex<LongWritable, BTGVertexValue, NullWritable> vertex,
    Iterable<NullWritable> messages) throws IOException {
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

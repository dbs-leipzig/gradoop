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
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.model.impl.operators.join.joinwithfusion.functions;

import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHead;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.model.api.functions.Function;

/**
 * Merge two heads belonging to the two graph operands
 * Created by vasistas on 15/02/17.
 */
public class MergeGraphHeads implements FlatJoinFunction<GraphHead, GraphHead, GraphHead> {

  /**
   * Reusable graph head. It consists on the final result
   */
  private final GraphHead gh;

  /**
   * Graph to which the newly created head belongs
   */
  private final GradoopId fusedGraphId;

  /**
   * Function allowing to concatenate graph heads
   */
  private final Function<Tuple2<String, String>, String> concatenateGraphHeads;

  /**
   * Function allowing to concatenate properties
   */
  private final Function<Tuple2<Properties, Properties>, Properties> concatenateProperties;

  /**
   * Default constructor
   * @param fusedGraphId          Graph to which the newly created head belongs
   * @param concatenateGraphHeads Function allowing to concatenate graph heads
   * @param concatenateProperties Function allowing to concatenate properties
   */
  public MergeGraphHeads(GradoopId fusedGraphId,
    Function<Tuple2<String, String>, String> concatenateGraphHeads,
    Function<Tuple2<Properties, Properties>, Properties> concatenateProperties) {
    this.fusedGraphId = fusedGraphId;
    this.concatenateGraphHeads = concatenateGraphHeads;
    this.concatenateProperties = concatenateProperties;
    gh = new GraphHead();
  }

  @Override
  public void join(GraphHead first, GraphHead second, Collector<GraphHead> out) throws
    Exception {
    gh.setId(fusedGraphId);
    gh.setLabel(concatenateGraphHeads.apply(new Tuple2<>(first.getLabel(), second.getLabel())));
    gh.setProperties(concatenateProperties.apply(new Tuple2<>(first.getProperties(), second
      .getProperties())));
    out.collect(gh);
  }
}

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
package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.types.NullValue;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Edge;

/**
 * turns an EPGM edge into a Gelly edge without data.
 */
public class ToGellyEdgeWithNullValue implements
  MapFunction<Edge, org.apache.flink.graph.Edge<GradoopId, NullValue>> {

  /**
   * Reduce instantiations
   */
  private final org.apache.flink.graph.Edge<GradoopId, NullValue> reuse;

  /**
   * Constructor
   */
  public ToGellyEdgeWithNullValue() {
    reuse = new org.apache.flink.graph.Edge<>();
    reuse.f2 = NullValue.getInstance();
  }

  @Override
  public org.apache.flink.graph.Edge<GradoopId, NullValue> map(Edge e)
      throws Exception {
    reuse.setSource(e.getSourceId());
    reuse.setTarget(e.getTargetId());
    return reuse;
  }
}

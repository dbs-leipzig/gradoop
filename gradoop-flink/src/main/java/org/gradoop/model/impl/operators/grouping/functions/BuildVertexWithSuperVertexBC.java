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

package org.gradoop.model.impl.operators.grouping.functions;

import com.google.common.collect.Maps;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.configuration.Configuration;
import org.gradoop.model.impl.id.GradoopId;
import org.gradoop.model.impl.operators.grouping.tuples.VertexGroupItem;
import org.gradoop.model.impl.operators.grouping.tuples.VertexWithSuperVertex;
import org.gradoop.model.impl.tuples.IdWithIdSet;

import java.util.List;
import java.util.Map;

/**
 * Maps a {@link VertexGroupItem} to a {@link VertexWithSuperVertex} according
 * the the broadcasted mapping information.
 */
@FunctionAnnotation.ForwardedFields(
  "f0" // vertex id
)
public class BuildVertexWithSuperVertexBC
  extends RichMapFunction<VertexGroupItem, VertexWithSuperVertex> {
  /**
   * Broadcast variable name
   */
  public static final String BC_MAPPING = "mapping";
  /**
   * Reduce object instantiation
   */
  private final VertexWithSuperVertex reuseTuple;
  /**
   * Map from final super vertex id to a set of super vertex ids representing
   * the same group. The information is used to determine the correct super
   * vertex id for each incoming {@link VertexGroupItem}.
   */
  private List<IdWithIdSet> mapping;
  /**
   * Maps intermediate super vertex ids to their final super vertex id.
   */
  private Map<GradoopId, GradoopId> cache;
  /**
   * Creates the mapper
   */
  public BuildVertexWithSuperVertexBC() {
    this.reuseTuple = new VertexWithSuperVertex();
    this.cache      = Maps.newConcurrentMap();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    mapping = getRuntimeContext().getBroadcastVariable(BC_MAPPING);
  }

  @Override
  public VertexWithSuperVertex map(VertexGroupItem item) throws Exception {
    reuseTuple.setVertexId(item.getVertexId());
    reuseTuple.setSuperVertexId(getFinalGroupRepresentative(
      item.getSuperVertexId()));
    return reuseTuple;
  }

  /**
   * Determines the final super vertex id for the given id.
   *
   * @param current current super vertex id of a vertex
   * @return final super vertex id
   */
  private GradoopId getFinalGroupRepresentative(GradoopId current) {
    GradoopId result = null;
    if (cache.containsKey(current)) {
      result = cache.get(current);
    } else {
      for (IdWithIdSet group : mapping) {
        if (group.getIdSet().contains(current)) {
          result = group.f0;
          cache.put(current, result);
        }
      }
    }
    return result;
  }
}

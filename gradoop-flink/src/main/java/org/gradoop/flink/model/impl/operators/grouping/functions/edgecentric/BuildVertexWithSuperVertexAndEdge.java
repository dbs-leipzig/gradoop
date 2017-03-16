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

package org.gradoop.flink.model.impl.operators.grouping.functions.edgecentric;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.SuperEdgeGroupItem;
import org.gradoop.flink.model.impl.operators.grouping.tuples.edgecentric.VertexWithSuperVertexAndEdge;

import java.util.Set;

public class BuildVertexWithSuperVertexAndEdge implements
  FlatMapFunction<SuperEdgeGroupItem, VertexWithSuperVertexAndEdge> {

  /**
   * Avoid object instantiation.
   */
  private final VertexWithSuperVertexAndEdge reuseTuple;

  public BuildVertexWithSuperVertexAndEdge() {
    reuseTuple = new VertexWithSuperVertexAndEdge();
  }

  @Override
  public void flatMap(SuperEdgeGroupItem superEdgeGroupItem,
    Collector<VertexWithSuperVertexAndEdge> collector) throws Exception {
    Set<GradoopId> sources = superEdgeGroupItem.getSourceIds();
    Set<GradoopId> targets = superEdgeGroupItem.getTargetIds();

    if (sources.size() != 1) {
      reuseTuple.setSuperVertexId(GradoopId.get());
    } else {
      reuseTuple.setSuperVertexId(sources.iterator().next());
    }
    reuseTuple.setSuperEdgeId(superEdgeGroupItem.getEdgeId());
    for (GradoopId source : sources) {
      reuseTuple.setVertexId(source);
      reuseTuple.setSource(true);
      collector.collect(reuseTuple);
    }
    if (targets.size() != 1) {
      reuseTuple.setSuperVertexId(GradoopId.get());
    } else {
      reuseTuple.setSuperVertexId(targets.iterator().next());
    }
    for (GradoopId target : targets) {
      reuseTuple.setVertexId(target);
      reuseTuple.setSource(false);
      collector.collect(reuseTuple);
    }
  }
}
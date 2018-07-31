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
package org.gradoop.flink.model.impl.operators.fusion.functions;

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.Vertex;

import java.util.Collection;

/**
 * Associates the old vertices with the new vertex fused ids.
 *
 */
@FunctionAnnotation.ForwardedFieldsFirst("f0->f0")
@FunctionAnnotation.ReadFieldsSecond("f0")
public class CoGroupAssociateOldVerticesWithNewIds implements
  CoGroupFunction<Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>, Tuple2<Vertex, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<Vertex, GradoopId> reusable;

  /**
   * Reusable collection for traversing the second operand
   */
  private final Collection<GradoopId> reusableList;

  /**
   * Default constructor
   */
  public CoGroupAssociateOldVerticesWithNewIds() {
    reusable = new Tuple2<>();
    reusableList = Lists.newArrayList();
  }

  @Override
  public void coGroup(Iterable<Tuple2<Vertex, GradoopId>> first,
    Iterable<Tuple2<Vertex, GradoopId>> second, Collector<Tuple2<Vertex, GradoopId>> out)
      throws Exception {
    reusableList.clear();
    second.forEach(x -> reusableList.add(x.f0.getId()));
    for (Tuple2<Vertex, GradoopId> x : first) {
      for (GradoopId y : reusableList) {
        reusable.f0 = x.f0;
        reusable.f1 = y;
        out.collect(reusable);
      }
    }
  }
}

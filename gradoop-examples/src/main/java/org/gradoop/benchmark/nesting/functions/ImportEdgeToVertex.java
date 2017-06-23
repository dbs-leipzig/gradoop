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
package org.gradoop.benchmark.nesting.functions;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * It flattens each edge and generates vertices from the source and the target description
 *
 * @param <K> user-defined Id representation
 */
@FunctionAnnotation.ForwardedFields("f1 -> f1")
public class ImportEdgeToVertex<K extends Comparable<K>>
  implements FlatMapFunction<Tuple2<ImportEdge<K>, GradoopId>,
             Tuple2<ImportVertex<K>, GradoopId>> {

  /**
   * Reusable element
   */
  private final Tuple2<ImportVertex<K>, GradoopId> reusable;

  /**
   * Default constructor
   */
  public ImportEdgeToVertex() {
    reusable = new Tuple2<>();
    reusable.f0 = new ImportVertex<>();
    reusable.f0.setLabel("");
    reusable.f0.setProperties(new Properties());
  }

  @Override
  public void flatMap(Tuple2<ImportEdge<K>, GradoopId>              value,
                      Collector<Tuple2<ImportVertex<K>, GradoopId>> out)
    throws Exception {
    reusable.f0.setId(value.f0.getSourceId());
    reusable.f1 = value.f1;
    reusable.f0.setLabel(value.f0.getSourceId().toString());
    out.collect(reusable);
    reusable.f0.setId(value.f0.getTargetId());
    reusable.f0.setLabel(value.f0.getTargetId().toString());
    reusable.f1 = value.f1;
    out.collect(reusable);
  }
}

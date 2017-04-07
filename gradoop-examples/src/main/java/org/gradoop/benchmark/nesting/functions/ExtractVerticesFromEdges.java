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
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Maps each edge to a vertex, both source and destination
 *
 * @param <K> Types identifying the key
 */
@FunctionAnnotation.ForwardedFieldsSecond("* -> f0")
public class ExtractVerticesFromEdges<K extends Comparable<K>>
  implements FlatMapFunction<Tuple2<ImportEdge<K>, GradoopId>,
                             Tuple2<ImportVertex<K>, GradoopId>> {

  private final Tuple2<ImportVertex<K>, GradoopId> src;
  private final Tuple2<ImportVertex<K>, GradoopId> dst;

  public ExtractVerticesFromEdges() {
    src = new Tuple2<>();
    src.f0 = new ImportVertex<>();
    dst = new Tuple2<>();
    dst.f0 = new ImportVertex<>();
  }

  @Override
  public void flatMap(Tuple2<ImportEdge<K>, GradoopId> kImportEdge,
                      Collector<Tuple2<ImportVertex<K>, GradoopId>>
    collector) throws
    Exception {
    src.f1 = kImportEdge.f1;
    dst.f1 = kImportEdge.f1;
    src.f0.setId(kImportEdge.f0.getSourceId());
    dst.f0.setId(kImportEdge.f0.getTargetId());
    collector.collect(src);
    collector.collect(dst);
  }

}

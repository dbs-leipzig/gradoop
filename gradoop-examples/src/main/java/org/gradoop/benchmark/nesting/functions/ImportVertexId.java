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

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Distinguishes a ImportVertex by its id
 * @param <K> vertex id
 */
public class ImportVertexId<K extends Comparable<K>>
  implements KeySelector<Tuple2<ImportVertex<K>, GradoopId>, K>,
             MapFunction<K, ImportVertex<K>> {
  @Override
  public K getKey(Tuple2<ImportVertex<K>, GradoopId> kImportVertex) throws Exception {
    return kImportVertex.f0.getId();
  }

  @Override
  public ImportVertex<K> map(K k) throws Exception {
    return new ImportVertex<K>(k);
  }
}

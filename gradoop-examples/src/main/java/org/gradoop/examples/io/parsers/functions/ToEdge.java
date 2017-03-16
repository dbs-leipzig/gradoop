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

package org.gradoop.examples.io.parsers.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.examples.io.parsers.inputfilerepresentations.Edgable;
import org.gradoop.flink.model.api.functions.Function;

/**
 * Converts an object that could be represented as a vertex (Vertexable) into a
 * proper vertex instance.
 *
 * @param <K> Comparable Type
 * @param <X> Edge Type
 */
public class ToEdge<K extends Comparable<K>, X extends Edgable<K>> implements
  MapFunction<X, ImportEdge<K>> {

  /**
   * Reusable element
   */
  private final ImportEdge<K> reusable = new ImportEdge<>();

  /**
   * Converting the GradoopId into the default id
   */
  private Function<GradoopId, K> conversion;

  /**
   * Default constructor
   * @param conversion  Element applying the conversion
   */
  public ToEdge(Function<GradoopId, K> conversion) {
    this.conversion = conversion;
  }

  @Override
  public ImportEdge<K> map(X value) throws Exception {
    reusable.setId(conversion.apply(GradoopId.get()));
    reusable.setProperties(value);
    reusable.setLabel(value.getLabel());
    return reusable;
  }
}

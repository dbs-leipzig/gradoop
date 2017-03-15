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

package org.gradoop.flink.io.reader.parsers;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.pojo.GraphHeadFactory;

import java.util.List;

/**
 * Associates to each group a different id
 * @param <Element> the element type that has defines a key identifier for the GraphClob
 */
@FunctionAnnotation.ForwardedFieldsSecond("* -> f1")
public class ExtendListOfElementWithId<Element> implements
  FlatMapFunction<List<Element>, Tuple2<GradoopId, Element>> {

  /**
   * Reusable element
   */
  private final Tuple2<GradoopId, Element> reusable;

  /**
   * Defines the graph head factory for defining new graphs
   */
  private final GraphHeadFactory fact;

  /**
   * Default constructor
   * @param fact  The GraphHead factory used for generating graph heads
   */
  public ExtendListOfElementWithId(GraphHeadFactory fact) {
    reusable = new Tuple2<>();
    this.fact = fact;
  }

  @Override
  public void flatMap(List<Element> value, Collector<Tuple2<GradoopId, Element>> out) throws
    Exception {
    reusable.f0 = fact.createGraphHead().getId();
    for (Element x : value) {
      reusable.f1 = x;
      out.collect(reusable);
    }
  }
}

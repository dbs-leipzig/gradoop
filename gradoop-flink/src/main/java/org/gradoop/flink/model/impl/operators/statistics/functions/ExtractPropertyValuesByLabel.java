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

package org.gradoop.flink.model.impl.operators.statistics.functions;

import com.google.common.collect.Sets;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.pojo.GraphElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.util.Set;

/**
 * Extracts Triples of the form <Tuple2<Label,PropertyName>, PropertyValue> from the given list of
 * GraphElements
 * @param <T> graph element type
 */
@FunctionAnnotation.ForwardedFields("label->0.0")
public class ExtractPropertyValuesByLabel<T extends GraphElement>
  extends RichFlatMapFunction<T, Tuple2<Tuple2<String, String>, Set<PropertyValue>>> {

  /**
   * Reuse Tuple
   */
  private final Tuple2<Tuple2<String, String>, Set<PropertyValue>> reuseTuple;

  /**
   * Creates a new UDF
   */
  public ExtractPropertyValuesByLabel() {
    this.reuseTuple = new Tuple2<>();
    this.reuseTuple.f0 = new Tuple2<>();
  }

  @Override
  public void flatMap(T value, Collector<Tuple2<Tuple2<String, String>, Set<PropertyValue>>> out)
      throws Exception {

    for (Property property : value.getProperties()) {
      reuseTuple.f0.f0 = value.getLabel();
      reuseTuple.f0.f1 = property.getKey();
      reuseTuple.f1 = Sets.newHashSet(property.getValue());

      out.collect(reuseTuple);
    }
  }
}

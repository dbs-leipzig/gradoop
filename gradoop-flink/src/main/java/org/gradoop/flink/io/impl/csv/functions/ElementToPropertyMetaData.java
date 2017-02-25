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

package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.FunctionAnnotation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.impl.pojo.Element;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.flink.io.impl.csv.metadata.MetaDataParser;

import java.util.HashSet;
import java.util.Set;

/**
 * (element) -> (elementLabel, {key_1:type_1,key_2:type_2,...,key_n:type_n})
 *
 * @param <E> EPGM element type
 */
@FunctionAnnotation.ForwardedFields("label->f0")
public class ElementToPropertyMetaData<E extends Element> implements MapFunction<E, Tuple2<String, Set<String>>> {
  /**
   * Reduce object instantiations.
   */
  private final Tuple2<String, Set<String>> reuseTuple;
  /**
   * Constructor
   */
  public ElementToPropertyMetaData() {
    reuseTuple = new Tuple2<>();
    reuseTuple.f1 = new HashSet<>();
  }

  @Override
  public Tuple2<String, Set<String>> map(E e) throws Exception {
    reuseTuple.f0 = e.getLabel();
    reuseTuple.f1.clear();
    for (Property property : e.getProperties()) {
      reuseTuple.f1.add(MetaDataParser.getPropertyMetaData(property));
    }
    return reuseTuple;
  }
}

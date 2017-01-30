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

import com.google.common.collect.Lists;
import org.apache.flink.api.common.functions.RichGroupCombineFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.flink.model.impl.tuples.WithCount;

import java.util.List;

/**
 * Combines list of <Property Name, Property Value> pairs to pairs with
 * <Property Name, Distinct Property Value Count>
 *
 */
public class CombinePropertyValueDistribution
  extends RichGroupCombineFunction<Tuple2<String, PropertyValue>, WithCount<String>> {

  @Override
  public void combine(Iterable<Tuple2<String, PropertyValue>> values,
    Collector<WithCount<String>> out) throws Exception {

    List<Tuple2<String, PropertyValue>> pairs = Lists.newArrayList(values);

    Long distinctValues = pairs.stream()
      .map(triple -> triple.f1)
      .distinct()
      .count();

    out.collect(new WithCount<>(pairs.get(0).f0, distinctValues));
  }
}

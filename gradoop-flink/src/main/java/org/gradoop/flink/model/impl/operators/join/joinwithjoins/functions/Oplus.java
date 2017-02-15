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

package org.gradoop.flink.model.impl.operators.join.joinwithjoins.functions;

import org.apache.flink.api.java.tuple.Tuple2;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.flink.model.api.functions.Function;

import java.io.Serializable;
import java.util.HashSet;
import java.util.function.Supplier;

/**
 * Defining a generic class for chaining two elments into once
 * @param <K>
 *
 * Created by Giacomo Bergami on 30/01/17.
 */
public abstract class  Oplus<K extends EPGMElement> implements Function<Tuple2<K, K>, K>,
  Serializable {

  /**
   * Defines a provider…
   * @return  a new instance of an element;
   */
  public abstract K supplyEmpty();

  /**
   * How to combine two labels from two EPGMElements
   * @param labelLeft   Left label
   * @param labelRight  Right label
   * @return            Concatenated label
   */
  public abstract String concatenateLabels(String labelLeft, String labelRight);

  public static <K extends EPGMElement> Oplus<K> generate(Supplier<K> supplier,
    Function<Tuple2<String, String>, String>
    labelConcatenation) {
    return new Oplus<K>() {
      @Override
      public K supplyEmpty() {
        return supplier.get();
      }

      @Override
      public String concatenateLabels(String labelLeft, String labelRight) {
        return labelConcatenation.apply(new Tuple2<>(labelLeft, labelRight));
      }

    };
  }

  @Override
  public K apply(Tuple2<K, K> k) {
    final K left = k.f0;
    final K right = k.f1;
   et = supplyEmpty();
   toret.setId(GradoopId.get());
   toret.setLabel(concatenateLabels(left.getLabel(), right.getLabel()));
   et<String> ll = new HashSet<String>();
   left.getProperties().getKeys().forEach(ll::add);
   et<String> rr = new HashSet<String>();
   right.getProperties().getKeys().forEach(rr::add);
   ll.retainAll(rr);
   for (String x : left.getProperties().getKeys()) {
     toret.setProperty(x, left.getPropertyValue(x));
   }
   for (String x : right.getProperties().getKeys()) {
     toret.setProperty(x, right.getPropertyValue(x));
   }
   return (toret);
  }

}

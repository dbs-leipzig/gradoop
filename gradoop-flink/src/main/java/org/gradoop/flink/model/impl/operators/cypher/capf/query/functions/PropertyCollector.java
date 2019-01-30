/*
 * Copyright © 2014 - 2019 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.cypher.capf.query.functions;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Create a mapping between labels of elements and all properties they have, described by their
 * names and classes.
 *
 * @param <E> an EPGMElement type
 */
public class PropertyCollector<E extends EPGMElement>
  implements GroupReduceFunction<E, Tuple3<String, String, Set<Tuple2<String, Class<?>>>>> {

  /**
   * Reduce object instantiation
   */
  private Tuple3<String, String, Set<Tuple2<String, Class<?>>>> reuseTuple = new Tuple3<>();

  /**
   * Create a new Property collector.
   *
   * @param elementType String specifying the type of elements.
   */
  public PropertyCollector(String elementType) {
    this.reuseTuple.f0 = elementType;
  }

  @Override
  public void reduce(
    Iterable<E> iterable,
    Collector<Tuple3<String, String, Set<Tuple2<String, Class<?>>>>> collector) throws Exception {

    Set<Tuple2<String, Class<?>>> properties = new HashSet<>();

    Iterator<E> it = iterable.iterator();

    if (!it.hasNext()) {
      return;
    }

    E first = it.next();
    String label = first.getLabel();

    addProperties(properties, first);

    while (it.hasNext()) {
      addProperties(properties, it.next());
    }

    reuseTuple.f1 = label;
    reuseTuple.f2 = properties;

    collector.collect(reuseTuple);

  }

  /**
   * Add all properties of an element to the property set.
   *
   * @param properties set of already found properties
   * @param element    the epgm element
   */
  private void addProperties(Set<Tuple2<String, Class<?>>> properties, E element) {
    if (element.getProperties() == null) {
      return;
    }

    for (Property p : element.getProperties()) {
      properties.add(new Tuple2<>(p.getKey(), p.getValue().getType()));
    }
  }
}

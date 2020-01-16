/*
 * Copyright © 2014 - 2020 Leipzig University (Database Research Group)
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
package org.gradoop.flink.model.impl.operators.keyedgrouping.keys;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.flink.model.api.functions.KeyFunction;

import java.util.List;
import java.util.Objects;

/**
 * A key function that combines other key functions by storing the values of the other key functions
 * in order in a tuple.
 *
 * @param <T> The type of the elements to group.
 */
public class CompositeKeyFunction<T> implements KeyFunction<T, Tuple> {

  /**
   * A list of grouping key functions combined in this key function.
   */
  private final List<? extends KeyFunction<T, ?>> componentFunctions;

  /**
   * Reduce object instantiations.
   */
  private final Tuple reuseTuple;

  /**
   * Create a new instance of this key function.
   *
   * @param keyFunctions The key functions to be combined.
   */
  public CompositeKeyFunction(List<? extends KeyFunction<T, ?>> keyFunctions) {
    this.componentFunctions = Objects.requireNonNull(keyFunctions);
    if (keyFunctions.size() > Tuple.MAX_ARITY) {
      throw new IllegalArgumentException("Too many keys. Maximum tuple arity exceeded: " +
        keyFunctions.size() + " (max.: " + Tuple.MAX_ARITY + ")");
    }
    reuseTuple = Tuple.newInstance(componentFunctions.size());
  }

  @Override
  public Tuple getKey(T element) {
    for (int index = 0; index < componentFunctions.size(); index++) {
      reuseTuple.setField(componentFunctions.get(index).getKey(element), index);
    }
    return reuseTuple;
  }

  @Override
  public void addKeyToElement(T element, Object key) {
    if (!(key instanceof Tuple)) {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
    for (int nr = 0; nr < componentFunctions.size(); nr++) {
      componentFunctions.get(nr).addKeyToElement(element, ((Tuple) key).getField(nr));
    }
  }

  @Override
  public TypeInformation<Tuple> getType() {
    TypeInformation[] types = new TypeInformation[componentFunctions.size()];
    for (int i = 0; i < types.length; i++) {
      types[i] = componentFunctions.get(i).getType();
    }
    return new TupleTypeInfo<>(types);
  }
}

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
package org.gradoop.flink.model.impl.operators.keyedgrouping.labelspecific;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.gradoop.common.model.api.entities.Element;
import org.gradoop.flink.model.api.functions.KeyFunction;
import org.gradoop.flink.model.api.functions.KeyFunctionWithDefaultValue;
import org.gradoop.flink.model.impl.operators.grouping.Grouping;
import org.gradoop.flink.model.impl.operators.keyedgrouping.keys.CompositeKeyFunctionWithDefaultValues;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A grouping key function that extracts grouping keys only for specific labels.
 *
 * @param <T> The type of the elements to group.
 */
public class LabelSpecificKeyFunction<T extends Element> implements KeyFunction<T, Tuple> {

  /**
   * A label used to identify the default groups.
   * If the label of an element is not a key in the map, the keys associated with label are used instead.
   * <p>
   * <i>Hint:</i> For backwards compatibility either {@link Grouping#DEFAULT_VERTEX_LABEL_GROUP} or
   * {@link Grouping#DEFAULT_EDGE_LABEL_GROUP} may be used instead.
   */
  public static final String DEFAULT_GROUP_LABEL = Grouping.DEFAULT_VERTEX_LABEL_GROUP;

  /**
   * A map assigning an internally used number to each label.
   */
  private final Map<String, Integer> labelToIndex;

  /**
   * A list of grouping key functions to be used for each label.
   * The {@code 0}th element of this list is the key function used for the default label group.
   */
  private final List<KeyFunctionWithDefaultValue<T, ?>> keyFunctions;

  /**
   * An array of labels to be set on super elements.
   */
  private final String[] targetLabels;

  /**
   * Reduce object instantiations.
   */
  private final Tuple reuseTuple;

  /**
   * Create an instance of this key function.
   *
   * @param labelsWithKeys    A map assigning a list of key functions to each label.
   * @param labelToSuperLabel A map assigning a label to be set on the super element for each label.
   */
  public LabelSpecificKeyFunction(Map<String, List<KeyFunctionWithDefaultValue<T, ?>>> labelsWithKeys,
    Map<String, String> labelToSuperLabel) {
    final boolean hasDefaultVertexGroup = labelsWithKeys.containsKey(Grouping.DEFAULT_VERTEX_LABEL_GROUP);
    final boolean hasDefaultEdgeGroup = labelsWithKeys.containsKey(Grouping.DEFAULT_EDGE_LABEL_GROUP);
    final String defaultGroupLabel;
    if (hasDefaultEdgeGroup && hasDefaultVertexGroup) {
      throw new IllegalArgumentException("The map contains both default label groups. Only one is expected.");
    } else if (hasDefaultVertexGroup) {
      defaultGroupLabel = Grouping.DEFAULT_VERTEX_LABEL_GROUP;
    } else if (hasDefaultEdgeGroup) {
      defaultGroupLabel = Grouping.DEFAULT_EDGE_LABEL_GROUP;
    } else {
      throw new IllegalArgumentException("The map contains no default label groups. One is expected.");
    }
    final int totalLabels = Objects.requireNonNull(labelsWithKeys).size();
    if (totalLabels + 1 > Tuple.MAX_ARITY) {
      throw new IllegalArgumentException("Too many labels. Tuple arity exceeded: " + (totalLabels + 1) +
        " (max.: " + Tuple.MAX_ARITY + ")");
    }
    int labelNr = 1;
    labelToIndex = new HashMap<>();
    // The list needs to be filled initially, the set(int,Object) function will fail otherwise.
    keyFunctions = new ArrayList<>(Collections.nCopies(totalLabels, null));
    targetLabels = new String[totalLabels];
    for (Map.Entry<String, List<KeyFunctionWithDefaultValue<T, ?>>> labelToKeys : labelsWithKeys.entrySet()) {
      final String key = labelToKeys.getKey();
      final List<KeyFunctionWithDefaultValue<T, ?>> keysForLabel = labelToKeys.getValue();
      if (key.equals(defaultGroupLabel)) {
        // Ensure that the keys for the default group are always the 0th position.
        keyFunctions.set(0, keysForLabel.size() == 1 ?
          keysForLabel.get(0) : new CompositeKeyFunctionWithDefaultValues<>(keysForLabel));
        continue;
      }
      labelToIndex.put(key, labelNr);
      targetLabels[labelNr] = key;
      keyFunctions.set(labelNr, keysForLabel.size() == 1 ?
        keysForLabel.get(0) : new CompositeKeyFunctionWithDefaultValues<>(keysForLabel));
      labelNr++;
    }
    if (labelToSuperLabel != null) {
      for (Map.Entry<String, String> labelUpdateEntry : labelToSuperLabel.entrySet()) {
        Integer index = labelToIndex.get(labelUpdateEntry.getKey());
        if (index == null) {
          continue;
        }
        targetLabels[index] = labelUpdateEntry.getValue();
      }
    }
    reuseTuple = Tuple.newInstance(1 + totalLabels);
  }

  @Override
  public Tuple getKey(T element) {
    Integer index = labelToIndex.get(element.getLabel());
    for (int i = 0; i < keyFunctions.size(); i++) {
      reuseTuple.setField(keyFunctions.get(i).getDefaultKey(), 1 + i);
    }
    if (index == null) {
      index = 0;
    }
    // The index is used to identify the key function, the 0th position in the tuple set to that index.
    reuseTuple.setField(keyFunctions.get(index).getKey(element), 1 + index);
    reuseTuple.setField(index, 0);
    return reuseTuple;
  }

  @Override
  public void addKeyToElement(T element, Object key) {
    if (!(key instanceof Tuple)) {
      throw new IllegalArgumentException("Invalid type for key: " + key.getClass().getSimpleName());
    }
    Integer index = ((Tuple) key).getField(0);
    keyFunctions.get(index).addKeyToElement(element, ((Tuple) key).getField(1 + index));
    if (index != 0) {
      element.setLabel(targetLabels[index]);
    }
  }

  @Override
  public TypeInformation<Tuple> getType() {
    TypeInformation[] types = new TypeInformation[1 + keyFunctions.size()];
    types[0] = BasicTypeInfo.INT_TYPE_INFO;
    for (int index = 0; index < keyFunctions.size(); index++) {
      types[1 + index] = keyFunctions.get(index).getType();
    }
    return new TupleTypeInfo<>(types);
  }
}

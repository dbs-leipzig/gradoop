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
package org.gradoop.flink.model.impl.operators.grouping.functions;

import org.gradoop.flink.model.api.functions.AggregateFunction;
import org.gradoop.common.model.api.entities.EPGMAttributed;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.common.model.impl.properties.PropertyValueList;
import org.gradoop.flink.model.impl.operators.grouping.tuples.LabelGroup;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;

/**
 * Encapsulates logic that is used for building summarized vertices and edges.
 */
abstract class BuildBase implements Serializable {

  /**
   * Class version for serialization.
   */
  private static final long serialVersionUID = 1L;

  /**
   * True, if the label shall be considered.
   */
  private final boolean useLabel;

  /**
   * Creates build base.
   *
   * @param useLabel use edge label
   */
  protected BuildBase(boolean useLabel) {
    this.useLabel = useLabel;

  }

  //----------------------------------------------------------------------------
  // Label
  //----------------------------------------------------------------------------

  /**
   * Returns true, if the label of the element shall be considered during summarization.
   *
   * @return true, iff the element label shall be considered
   */
  protected boolean useLabel() {
    return useLabel;
  }

  //----------------------------------------------------------------------------
  // Grouping properties
  //----------------------------------------------------------------------------

  /**
   * Adds the given group properties to the attributed element.
   *
   * @param attributed attributed element
   * @param groupPropertyValues group property values
   * @param vertexLabelGroup vertex label group
   */
  protected void setGroupProperties(EPGMAttributed attributed,
    PropertyValueList groupPropertyValues, LabelGroup vertexLabelGroup) {

    Iterator<PropertyValue> valueIterator = groupPropertyValues.iterator();

    for (String groupPropertyKey : vertexLabelGroup.getPropertyKeys()) {
      attributed.setProperty(groupPropertyKey, valueIterator.next());
    }
  }

  //----------------------------------------------------------------------------
  // Aggregation properties
  //----------------------------------------------------------------------------

  /**
   * Sets the given property values as new properties at the given element.
   * @param element attributed element
   * @param values aggregate values
   * @param valueAggregators aggregate functions
   */
  protected void setAggregateProperties(EPGMAttributed element, PropertyValueList values,
                                        List<AggregateFunction> valueAggregators) {
    if (!valueAggregators.isEmpty()) {
      Iterator<PropertyValue> valueIt = values.iterator();
      for (AggregateFunction valueAggregator : valueAggregators) {
        element.setProperty(valueAggregator.getAggregatePropertyKey(), valueIt.next());
      }
    }
  }
}

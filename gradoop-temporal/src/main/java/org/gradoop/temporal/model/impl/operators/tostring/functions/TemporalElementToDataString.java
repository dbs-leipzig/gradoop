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
package org.gradoop.temporal.model.impl.operators.tostring.functions;

import org.gradoop.flink.model.impl.operators.tostring.functions.ElementToDataString;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

/**
 * Superclass of data-bases string representations of temporal elements,
 * i.e. such including label, properties and valid time.
 *
 * @param <EL> temporal element type
 */
public class TemporalElementToDataString<EL extends TemporalElement> extends ElementToDataString<EL> {

  /**
   * Represents the time fields of the input element as a comma separated string.
   * Default times result in an empty string for this field.
   *
   * @param element temporal element
   * @return valid time and transaction time as of element as string
   */
  protected String time(EL element) {
    StringBuilder builder = new StringBuilder();
    builder.append('(');

    Long time = element.getValidFrom();
    if (!time.equals(TemporalElement.DEFAULT_TIME_FROM)) {
      builder.append(time);
    }
    builder.append(',');
    time = element.getValidTo();
    if (!time.equals(TemporalElement.DEFAULT_TIME_TO)) {
      builder.append(time);
    }

    builder.append(')');
    return builder.toString();
  }
}

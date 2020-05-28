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
package org.gradoop.examples.common.functions;

import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalElement;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.List;

/**
 * A transformation function applicable on a {@link TemporalElement} that converts all properties matching
 * the given list of keys from type {@link Long} to a {@link LocalDateTime} value.
 *
 * @param <TE> the type of the temporal element
 */
public class TransformLongPropertiesToDateTime<TE extends TemporalElement> implements TransformationFunction<TE> {
  /**
   * A list of property keys that will be converted.
   */
  private final List<String> propertyNames;

  /**
   * Creates an instance of this temporal transformation function.
   *
   * @param properties a set of property names which hold a {@link Long} value. Each value will be parsed
   *                 to a {@link LocalDateTime} value.
   */
  public TransformLongPropertiesToDateTime(String... properties) {
    this.propertyNames = Arrays.asList(properties);
  }

  @Override
  public TE apply(TE current, TE transformed) {
    transformed.setLabel(current.getLabel());
    for (String propKey : current.getPropertyKeys()) {
      if (propertyNames.contains(propKey) && current.getPropertyValue(propKey).isLong()) {
        LocalDateTime parsedValue = Instant.ofEpochMilli(current.getPropertyValue(propKey).getLong())
          .atZone(ZoneId.of("UTC"))
          .toLocalDateTime();
        transformed.setProperty(propKey, parsedValue);
      } else {
        transformed.setProperty(propKey, current.getPropertyValue(propKey));
      }
    }
    return transformed;
  }
}

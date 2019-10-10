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
package org.gradoop.examples.aggregation.functions;

import org.gradoop.flink.model.api.functions.TransformationFunction;
import org.gradoop.temporal.model.impl.pojo.TemporalGraphHead;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * A transformation function applicable on {@link TemporalGraphHead} instances that converts all properties of
 * type {@link Long} to a {@link LocalDateTime} value.
 */
public class TransformLongToDateTime implements TransformationFunction<TemporalGraphHead> {

  @Override
  public TemporalGraphHead apply(TemporalGraphHead current, TemporalGraphHead transformed) {
    transformed.setLabel(current.getLabel());
    for (String propKey : current.getPropertyKeys()) {
      if (current.getPropertyValue(propKey).isLong()) {
        LocalDateTime parsedValue = Instant.ofEpochMilli(current.getPropertyValue(propKey).getLong())
          .atZone(ZoneId.systemDefault())
          .toLocalDateTime();
        transformed.setProperty(propKey, parsedValue);
      } else {
        transformed.setProperty(propKey, current.getPropertyValue(propKey));
      }
    }
    return transformed;
  }
}

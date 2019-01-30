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

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.id.GradoopId;
import org.gradoop.common.model.impl.properties.Property;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Encodes property types not yet supported by CAPF to a String representation.
 * @param <E> an EPGMElement type
 */
public class PropertyEncoder<E extends EPGMElement> implements MapFunction<E, E> {


  @Override
  public E map(E e) throws Exception {
    if (e.getProperties() != null) {
      Class[] classes = {BigDecimal.class, GradoopId.class, Map.class, List.class, LocalDate.class,
        LocalTime.class, LocalDateTime.class, Set.class};
      Set<Class> classSet = new HashSet<>(Arrays.asList(classes));
      for (Property prop : e.getProperties()) {
        if (classSet.contains(prop.getValue().getType())) {
          e.getProperties().set(prop.getKey(),
            "CAPFProperty" + new String(prop.getValue().getRawBytes(), Charset.forName("UTF-8")));
        }
      }
    }
    return e;
  }
}

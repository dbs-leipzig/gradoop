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
package org.gradoop.flink.model.impl.operators.cypher.capf.result.functions;

import org.apache.flink.api.common.functions.MapFunction;
import org.gradoop.common.model.api.entities.EPGMElement;
import org.gradoop.common.model.impl.properties.Property;
import org.gradoop.common.model.impl.properties.PropertyValue;

import java.nio.charset.Charset;

/**
 * Decodes String representations of property types not yet supported by CAPF.
 *
 * @param <E> an EPGMElement type
 */
public class PropertyDecoder<E extends EPGMElement> implements MapFunction<E, E> {

  @Override
  public E map(E e) throws Exception {
    if (e.getProperties() != null) {
      for (Property prop : e.getProperties()) {
        PropertyValue val = prop.getValue();
        if (val.isString()) {
          if (val.getString().startsWith("CAPFProperty")) {
            String encoded = val.getString().substring(12);
            e.getProperties().set(
              prop.getKey(),
              PropertyValue.fromRawBytes(encoded.getBytes(Charset.forName("UTF-8"))));
          }
        }
      }
    }
    return e;
  }
}

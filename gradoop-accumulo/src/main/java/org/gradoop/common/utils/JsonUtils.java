/**
 * Copyright © 2014 - 2018 Leipzig University (Database Research Group)
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

package org.gradoop.common.utils;

import org.apache.htrace.fasterxml.jackson.annotation.JsonInclude;
import org.apache.htrace.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.htrace.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * json serialize utils
 */
public final class JsonUtils {

  /**
   * normal mapper
   */
  private static final ObjectMapper MAPPER = new ObjectMapper();


  static {
    MAPPER.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
  }

  /**
   * get common object mapper
   *
   * @return object mapper instance
   */
  public static ObjectMapper getMapper() {
    return MAPPER;
  }

  /**
   * dumps object to json string
   *
   * @param obj object instance
   * @return json string
   */
  public static String dumps(Object obj) {
    try {
      return MAPPER.writeValueAsString(obj);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * load json string to instance
   *
   * @param src json string
   * @param type object type
   * @param <T> type template
   * @return deserialize instance
   */
  public static <T> T loads(String src, Class<T> type) {
    try {
      return MAPPER.readValue(src, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * load json bytes to instance
   *
   * @param src json bytes
   * @param type object type
   * @param <T> type template
   * @return deserialize instance
   */
  public static <T> T loads(byte[] src, Class<T> type) {
    try {
      return MAPPER.readValue(src, type);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

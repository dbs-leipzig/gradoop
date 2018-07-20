/*
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
package org.gradoop.common.exceptions;

/**
 * Used during parsing of properties. Is thrown when a type is not supported by
 * Gradoop.
 */
public class UnsupportedTypeException extends RuntimeException {
  /**
   * Creates a new exception object using the given message.
   *
   * @param message exception message
   */
  public UnsupportedTypeException(String message) {
    super(message);
  }

  /**
   * Creates a new exception with information about the given class.
   *
   * @param clazz unsupported class
   */
  public UnsupportedTypeException(Class clazz) {
    super(String.format("Unsupported type: %s", clazz.getCanonicalName()));
  }
}

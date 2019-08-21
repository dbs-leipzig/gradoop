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
package org.gradoop.common.model.impl.properties;

/**
 * Representations of types that are supported by
 * {@link org.gradoop.common.model.impl.properties.PropertyValue}
 */
public enum Type {
  /** Null type */
  NULL(0x00, "null"),
  /** Boolean type */
  BOOLEAN(0x01, "boolean"),
  /** Integer type */
  INTEGER(0x02, "int"),
  /** Long type */
  LONG(0x03, "long"),
  /** Float type */
  FLOAT(0x04, "float"),
  /** Double type */
  DOUBLE(0x05, "double"),
  /** String type */
  STRING(0x06, "string"),
  /** BigDecimal type */
  BIG_DECIMAL(0x07, "bigdecimal"),
  /** GradoopId type */
  GRADOOP_ID(0x08, "gradoopid"),
  /** Map type */
  MAP(0x09, "map"),
  /** List type */
  LIST(0x0a, "list"),
  /** Date type */
  DATE(0x0b, "localdate"),
  /** Time type */
  TIME(0x0c, "localtime"),
  /** DateTime type */
  DATE_TIME(0x0d, "localdatetime"),
  /** Short type */
  SHORT(0x0e, "short"),
  /** Set type */
  SET(0x0f, "set");

  /**
   * Byte representation
   */
  private final byte typeByte;
  /**
   * String representation
   */
  private final String typeString;

  /**
   * Constructs an enum type that represents a supported type.
   *
   * @param typeByte byte representation as int, is casted to byte
   * @param typeString string representation
   */
  Type(int typeByte, String typeString) {
    this.typeByte = (byte) typeByte;
    this.typeString = typeString;
  }

  /**
   * Returns the byte representation.
   *
   * @return type byte
   */
  public byte getTypeByte() {
    return typeByte;
  }

  /**
   * Returns the string representation.
   *
   * @return type string
   */
  @Override
  public String toString() {
    return typeString;
  }
}

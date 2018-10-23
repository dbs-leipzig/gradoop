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
package org.gradoop.flink.io.impl.csv;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * Constants needed for CSV parsing.
 */
public class CSVConstants {
  /**
   * Used to separate the tokens (id, label, values) in the CSV files.
   */
  public static final String TOKEN_DELIMITER = ";";
  /**
   * Used to separate the property values in the CSV files.
   */
  public static final String VALUE_DELIMITER = "|";
  /**
   * Used to separate lines in the output CSV files.
   */
  public static final String ROW_DELIMITER = System.getProperty("line.separator");
  /**
   * Used to separate entries of list types in the CSV files
   */
  public static final String LIST_DELIMITER = ",";
  /**
   * Used to separate key and value of maps in the CSV files.
   */
  public static final String MAP_SEPARATOR = "=";
  /**
   * Used to tag a graph head entity.
   */
  public static final String GRAPH_TYPE = "g";
  /**
   * Used to tag a vertex entity.
   */
  public static final String VERTEX_TYPE = "v";
  /**
   * Used to tag an edge entity.
   */
  public static final String EDGE_TYPE = "e";
  /**
   * System constant file separator.
   */
  public static final String DIRECTORY_SEPARATOR = System.getProperty("file.separator");
  /**
   * File name for indexed data.
   */
  public static final String SIMPLE_FILE = "data.csv";
  /**
   * Directory to store empty labels with indexed CSV.
   */
  public static final String DEFAULT_DIRECTORY = "_";
  /**
   * Characters to be escaped in csv strings.
   */
  public static final Set<Character> ESCAPED_CHARACTERS = ImmutableSet
    .of('\\', ';', ',', '|', ':', '\n', '=');
}

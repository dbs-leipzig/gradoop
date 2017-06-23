/*
 * This file is part of Gradoop.
 *
 * Gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.flink.io.impl.csv;

/**
 * Constants needed for CSV parsing.
 */
public class CSVConstants {
  /**
   * Used to separate the tokens (id, label, values) in the CSV file.
   */
  public static final String TOKEN_DELIMITER = ";";
  /**
   * Used to separate the property values in the CSV file.
   */
  public static final String VALUE_DELIMITER = "|";
  /**
   * Used to separate lines in the output CSV files.
   */
  public static final String ROW_DELIMITER = System.getProperty("line.separator");
}

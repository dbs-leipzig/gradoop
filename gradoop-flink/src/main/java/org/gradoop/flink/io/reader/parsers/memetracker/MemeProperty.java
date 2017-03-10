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

package org.gradoop.flink.io.reader.parsers.memetracker;

/**
 * Defines all the possible attributes for the vertex
 */
enum MemeProperty {
  /**
   * Web Page id
   */
  Id("P"),
  /**
   * Web Page Timestamp
   */
  Timestamp("T"),
  /**
   * Phrase associated to the web page
   */
  Phrase("Q"),
  /**
   * Backward links
   */
  RefersTo("L");

  /**
   * Default value
   */
  public final String param;

  /**
   * Default constructor
   * @param l
   */
  MemeProperty(String l) {
    this.param = l;
  }

  /**
   * From string to Enum
   * @param s String
   * @return  enum constant
   */
  public static MemeProperty fromString(String s) {
    for (MemeProperty x : MemeProperty.values()) {
      if (x.param.equals(s))
        return x;
    }
    return null;
  }

  @Override
  public String toString() {
    return param;
  }
}

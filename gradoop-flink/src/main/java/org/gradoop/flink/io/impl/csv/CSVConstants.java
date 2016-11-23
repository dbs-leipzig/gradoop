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
 * Constants needed for the CSV IO classes.
 */
public class CSVConstants {

  /**
   * Separator for 'key' property concatenation.
   */
  public static final String SEPARATOR_KEY = "_";
  /**
   * Replacement string for 'key' property.
   */
  public static final String ESCAPE_REPLACEMENT_KEY = "&lowbar;";
  /**
   * Separator for 'graphs' property concatenation.
   */
  public static final String SEPARATOR_GRAPHS = "%";
  /**
   * Replacement string for 'graphs' property.
   */
  public static final String ESCAPE_REPLACEMENT_GRAPHS = "&percnt;";
  /**
   * Separator for the label concatenation.
   */
  public static final String SEPARATOR_LABEL = ";";
  /**
   * Replacement string for the label.
   */
  public static final String ESCAPE_REPLACEMENT_LABEL = "&semi;";
  /**
   * Separator for the id part of the label.
   */
  public static final String SEPARATOR_ID = "@";
  /**
   * Replacement string the id part of the label.
   */
  public static final String ESCAPE_REPLACEMENT_ID = "&commat;";
  /**
   * Property key for the value containing an edge's source.
   */
  public static final String PROPERTY_KEY_SOURCE = "source";
  /**
   * Property key for the value containing an edge's target.
   */
  public static final String PROPERTY_KEY_TARGET = "target";
  /**
   * Property key for the value containing an element's graph ids.
   */
  public static final String PROPERTY_KEY_GRAPHS = "graphs";
  /**
   * Property key for the value containing an elements's key (i.e.
   * concatenated primary key).
   */
  public static final String PROPERTY_KEY_KEY = "key";
  /**
   * Broadcast variable to spread the ids graph keys with their ids.
   */
  public static final String BROADCAST_ID_MAP = "idMap";
  /**
   * Broadcast variable to spread the graph heads.
   */
  public static final String BROADCAST_GRAPHHEADS = "graphHeads";
}

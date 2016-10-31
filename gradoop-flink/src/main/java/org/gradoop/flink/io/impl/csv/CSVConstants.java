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


public class CSVConstants {

  public static final String SEPARATOR_KEY = "_";

  public static final String ESCAPE_REPLACEMENT_KEY = "&lowbar;";

  public static final String SEPARATOR_GRAPHS = "%";

  public static final String ESCAPE_REPLACEMENT_GRAPHS = "&percnt;";

  public static final String SEPARATOR_LABEL = ";";

  public static final String ESCAPE_REPLACEMENT_LABEL = "&semi;";

  public static final String PROPERTY_KEY_SOURCE = "source";

  public static final String PROPERTY_KEY_TARGET = "target";

  public static final String PROPERTY_KEY_GRAPHS = "graphs";

  public static final String PROPERTY_KEY_KEY = "key";
}

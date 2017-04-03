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

package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple5;

/**
 * Tuple representing an edge in a CSV file.
 */
public class CSVEdge extends Tuple5<String, String, String, String, String> {

  public String getId() {
    return f0;
  }

  public void setId(String id) {
    f0 = id;
  }

  public String getSourceId() {
    return f1;
  }

  public void setSourceId(String sourceId) {
    f1 = sourceId;
  }

  public String getTargetId() {
    return f2;
  }

  public void setTargetId(String targetId) {
    f2 = targetId;
  }

  public String getLabel() {
    return f3;
  }

  public void setLabel(String label) {
    f3 = label;
  }

  public String getProperties() {
    return f4;
  }

  public void setProperties(String properties) {
    f4 = properties;
  }
}

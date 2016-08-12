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

package org.gradoop.flink.algorithms.fsm.gspan.encoders.comparators;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Map;

/**
 * Comparator for (label, frequency)
 */
public class LabelFrequencyEntryComparator
  implements Comparator<Map.Entry<String, Integer>>, Serializable {

  @Override
  public int compare(
    Map.Entry<String, Integer> label1, Map.Entry<String, Integer> label2) {

    int comparison;

    if (label1.getValue() > label2.getValue()) {
      comparison = -1;
    } else if (label1.getValue() < label2.getValue()) {
      comparison = 1;
    } else {
      comparison = label1.getKey().compareTo(label2.getKey());
    }

    return comparison;
  }
}


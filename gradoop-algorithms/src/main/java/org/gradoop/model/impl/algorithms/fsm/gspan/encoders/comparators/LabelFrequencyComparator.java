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

package org.gradoop.model.impl.algorithms.fsm.gspan.encoders.comparators;

import org.gradoop.model.impl.tuples.WithCount;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Comparator for (label, frequency)
 */
public class LabelFrequencyComparator
  implements Comparator<WithCount<String>>, Serializable {

  @Override
  public int compare(WithCount<String> label1, WithCount<String> label2) {

    int comparison;

    if (label1.getCount() > label2.getCount()) {
      comparison = -1;
    } else if (label1.getCount() < label2.getCount()) {
      comparison = 1;
    } else {
      comparison = label1.getObject().compareTo(label2.getObject());
    }

    return comparison;
  }
}


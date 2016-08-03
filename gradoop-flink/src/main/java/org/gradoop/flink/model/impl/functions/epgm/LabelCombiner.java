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

package org.gradoop.flink.model.impl.functions.epgm;

import org.apache.flink.api.common.functions.JoinFunction;
import org.gradoop.common.model.api.entities.EPGMLabeled;

/**
 * concatenates the labels of labeled things
 * @param <L> labeled type
 */
public class LabelCombiner<L extends EPGMLabeled> implements
  JoinFunction<L, L, L> {

  @Override
  public L join(L left, L right) throws Exception {

    String rightLabel = right == null ? "" : right.getLabel();

    left.setLabel(left.getLabel() + rightLabel);

    return left;
  }
}

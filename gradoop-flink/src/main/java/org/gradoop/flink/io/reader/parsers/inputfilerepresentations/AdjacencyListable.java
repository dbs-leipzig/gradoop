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

package org.gradoop.flink.io.reader.parsers.inputfilerepresentations;

import org.gradoop.flink.io.reader.parsers.ResettableIterator;

/**
 * Defines an element that could be represented as an adjacency list for a specific vertex, which
 * the element itself represents
 *
 * @param <Id> element defining the id
 * @param <Edge> element definign the edge
 */
public abstract class AdjacencyListable<Id extends Comparable<Id>, Edge extends Edgable<Id>> extends
  Vertexable<Id> implements ResettableIterator<Edge> {

}

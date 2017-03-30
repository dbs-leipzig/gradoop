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

/**
 * An nesting operator is a binary operator: taking as a first input the graph that has to be
 * nested and the graph collection containing the elements that will be nested, returns a graph
 * summarizing as a single vertex each set of vertices appearing in each element of the graph
 * collection
 */
package org.gradoop.flink.model.impl.operators.nest2.operator;

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
 * An fusion operator is a binary operator: taking a pattern graph (LogicalGraph) to be found into a
 * to-be-searched graph, it fuses it into one single vertex within the search graph. This package
 * provides the implementation of both VertexFusion and its reduced version, ReduceVertexFusion
 */
package org.gradoop.flink.model.impl.operators.fusion;

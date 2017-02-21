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
 * Provides an implementation for the join operand in different possible ways. A join is a binary
 * operand arbitrarly combining the two operands by using different semantics for both the
 * vertices and the edges.
 *
 * This subpackage provides the join with joins definition, that is the formal definition with
 * some enhancements (hashing function). Plus, it implements the R-join definition.
 */
package org.gradoop.flink.model.impl.operators.fusion.reduce.functions;

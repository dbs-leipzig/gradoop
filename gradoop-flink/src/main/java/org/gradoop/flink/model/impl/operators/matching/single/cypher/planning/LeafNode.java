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

package org.gradoop.flink.model.impl.operators.matching.single.cypher.planning;

/**
 * Represents a leaf node in the query plan. Leaf nodes are different in terms of their input which
 * is a data set containing EPGM elements, i.e. {@link org.gradoop.common.model.impl.pojo.Vertex} or
 * {@link org.gradoop.common.model.impl.pojo.Edge}.
 */
public interface LeafNode extends PlanNode {

}

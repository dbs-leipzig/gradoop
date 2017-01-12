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
package org.gradoop.flink.model.impl.operators.matching.common;

/**
 * Used to select the strategy used by the matching algorithms
 */
public enum MatchStrategy {
    /**
     * If this strategy is used vertices and edges can only be
     * mapped to one vertices/edges in the query graph
     */
    ISOMORPHISM,
    /**
     * If this strategy is used vertices and edges can be
     * mapped to multiple vertices/edges in the query graph
     */
    HOMOMORPHISM,
    /**
     * Match cypher style: Homomorphism for vertices, isomorphism for edges
     */
    CYPHER
}

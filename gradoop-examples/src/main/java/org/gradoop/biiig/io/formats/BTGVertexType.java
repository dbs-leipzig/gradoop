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
 * along with Gradoop.  If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.biiig.io.formats;

/**
 * Used for {@link org.gradoop.biiig.algorithms.BTGComputation}.
 */
public enum BTGVertexType {
  /**
   * Vertices that are created during a business transaction, like
   * invoices, quotations, deliveries.
   */
  TRANSACTIONAL {
    @Override
    public String toString() {
      return "TransData";
    }
  },
  /**
   * Vertices that take part in a business transaction, like users, products,
   * vendors.
   */
  MASTER {
    @Override
    public String toString() {
      return "MasterData";
    }
  }
}

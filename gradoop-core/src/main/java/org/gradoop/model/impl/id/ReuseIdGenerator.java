/*
 * This file is part of gradoop.
 *
 * gradoop is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * gradoop is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with gradoop. If not, see <http://www.gnu.org/licenses/>.
 */

package org.gradoop.model.impl.id;

/**
 * Superclass of Gradoop ID generators, which reuse integrity of existing
 * identifiers.
 */
public abstract class ReuseIdGenerator extends GradoopIdGeneratorBase {

  /**
   * Constructor.
   *
   * @param creatorId identifier of ID generation session
   * @param context generation context
   */
  public ReuseIdGenerator(int creatorId, Context context) {
    super(creatorId, context);
  }

  /**
   * Returns a new Gradoop ID based on a given existing identifier.
   *
   * @param reuseId existing identifier
   * @return new Gradoop ID
   */
  public GradoopId createId(long reuseId) {
    return new GradoopId(reuseId, creatorId, context);
  }
}

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
 * Generates Gradoop IDs sequentially during workflow execution
 */
public class SequenceIdGenerator extends GradoopIdGeneratorBase {

  /**
   * Current sequence number, increased with every generated id.
   */
  private long currentSequenceNumber;

  /**
   * Convenient constructor, starting at sequence number ZERO.
   *
   * @param creatorId worker-thread identifier
   * @param context generation context
   */
  public SequenceIdGenerator(int creatorId, Context context) {
    this(0L, creatorId, context);
  }

  /**
   * Constructor.
   *
   * @param offset initial sequence number
   * @param creatorId worker-thread identifier
   * @param context generation context
   */
  public SequenceIdGenerator(long offset, int creatorId, Context context) {
    super(creatorId, context);
    this.currentSequenceNumber = offset;
  }

  /**
   * Returns a new Gradoop ID and increases the current sequent number.
   *
   * @return new Gradoop ID
   */
  public GradoopId createId() {
    return new GradoopId(currentSequenceNumber++, creatorId, context);
  }
}

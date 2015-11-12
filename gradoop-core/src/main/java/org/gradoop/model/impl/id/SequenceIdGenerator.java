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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Creates a sequence of GradoopIds starting from a given offset.
 */
public class SequenceIdGenerator implements GradoopIdGenerator {

  /**
   * Thread-safe offset to create new identifers from.
   */
  private final AtomicLong offset;

  /**
   * Instantiates a new generator.
   */
  public SequenceIdGenerator() {
    this(0L);
  }

  /**
   * Instantiates a new generator.
   *
   * @param offset new identifiers are created based on that offset
   */
  public SequenceIdGenerator(Long offset) {
    if (offset == null) {
      throw new IllegalArgumentException("Offset must not be null");
    }
    this.offset = new AtomicLong(offset);
  }

  @Override
  public GradoopId createId() {
    return new GradoopId(offset.getAndIncrement());
  }
}

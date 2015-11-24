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
 * Generates Gradoop IDs based on existing identifiers in data imports.
 */
public class ImportIdGenerator extends ReuseIdGenerator {

  /**
   * Default constructor; sets context to IMPORT.
   */
  public ImportIdGenerator() {
    this(Context.IMPORT);
  }

  /**
   * Master constructor; sets initialization timestamp as import identifier.
   *
   * @param context generation context
   */
  public ImportIdGenerator(Context context) {
    super((int) System.currentTimeMillis() / 1000, context);
  }
}

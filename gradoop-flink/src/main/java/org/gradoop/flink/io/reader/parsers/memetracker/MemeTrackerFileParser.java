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

package org.gradoop.flink.io.reader.parsers.memetracker;

import org.gradoop.flink.io.reader.parsers.AdjacencyListFileParser;
import org.gradoop.flink.io.reader.parsers.memetracker.functions.FromMemeAdjacencyToEdges;
import org.gradoop.flink.io.reader.parsers.memetracker.functions.FromMemeEntryToVertex;
import org.gradoop.flink.io.reader.parsers.memetracker.functions.FromStringToMemeAdjacency;

/**
 * Implements the AdjacencyListFileParser with the default configurations
 */
public class MemeTrackerFileParser extends AdjacencyListFileParser<String, MemeTrackerEdge, MemeTrackerRecordParser> {
  /**
   * Default constructor
   */
  public MemeTrackerFileParser() {
    super(new FromStringToMemeAdjacency(),
          new FromMemeEntryToVertex(), new FromMemeAdjacencyToEdges());
    super.splitWith("\n\n");
  }
}

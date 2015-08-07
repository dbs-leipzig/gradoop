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

package org.gradoop.model.helper;

/**
 * Constants required by operator implementations in Flink.
 */
public class FlinkConstants {
  /**
   * Identifier of the database graph.
   */
  public static final Long DATABASE_GRAPH_ID = -1L;

  /**
   * Temporary identifier of a graph created by combine.
   */
  public static final Long COMBINE_GRAPH_ID = -2L;

  /**
   * Temporary identifier of a graph created by overlap.
   */
  public static final Long OVERLAP_GRAPH_ID = -3L;

  /**
   * Temporary identifier of a graph created by exclude.
   */
  public static final Long EXCLUDE_GRAPH_ID = -4L;

  /**
   * Temporary identifier of a graph created by summarize.
   */
  public static final Long SUMMARIZE_GRAPH_ID = -5L;
}

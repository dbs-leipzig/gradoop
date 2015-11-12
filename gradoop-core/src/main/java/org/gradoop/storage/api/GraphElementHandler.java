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

package org.gradoop.storage.api;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.gradoop.model.api.EPGMGraphElement;
import org.gradoop.model.impl.id.GradoopIds;

import java.io.IOException;
import java.util.Set;

/**
 * Responsible for reading and writing graph element entity data from and to
 * HBase. These are usually vertex and edge data objects.
 */
public interface GraphElementHandler extends ElementHandler {
  /**
   * Adds the given graph identifiers to the given {@link Put} and returns it.
   *
   * @param put          {@link Put} to add graph identifiers to
   * @param graphElement graph element
   * @return put with graph identifiers
   */
  Put writeGraphIds(final Put put, final EPGMGraphElement graphElement) throws
    IOException;

  /**
   * Reads the graph identifiers from the given {@link Result}.
   *
   * @param res HBase row
   * @return graphs identifiers
   */
  GradoopIds readGraphIds(final Result res) throws IOException;
}

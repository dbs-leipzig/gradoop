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

package org.gradoop.examples.io.parsers;

import org.apache.flink.api.common.io.DelimitedInputFormat;

import java.io.IOException;

/**
 * Reading the file, delimiter by delimiter
 */
public class ParametricInputFormat extends DelimitedInputFormat<String> {

  @Override
  public String readRecord(String reuse, byte[] bytes, int offset, int numBytes)
    throws IOException {
    return new String(bytes, offset, numBytes, "UTF-8");
  }

}

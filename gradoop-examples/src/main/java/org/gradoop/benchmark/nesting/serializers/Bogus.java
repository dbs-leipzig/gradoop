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
package org.gradoop.benchmark.nesting.serializers;


import org.apache.flink.api.common.io.RichOutputFormat;

import java.io.IOException;

/**
 * Bogus class, doing nothing at all, except by consuming the data
 * @param <T> Any data type will do
 */
public  class Bogus<T> extends RichOutputFormat<T> {
  @Override
  public void configure(org.apache.flink.configuration.Configuration parameters) {
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {

  }

  @Override
  public void writeRecord(T record) throws IOException {

  }

  @Override
  public void close() throws IOException {

  }
}

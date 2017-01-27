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

package org.gradoop.flink.model.impl.operators.fusion;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vasistas on 27/01/17.
 */
public class NotThrowingCollect<T> implements OutputFormat<T> {

  public final List<T> toret;

  public NotThrowingCollect() {
    toret = new ArrayList<T>();
  }

  @Override
  public void configure(Configuration parameters) {
  }

  @Override
  public void open(int taskNumber, int numTasks) throws IOException {
  }

  @Override
  public void writeRecord(T record) throws IOException {
    toret.add(record);
  }

  @Override
  public void close() throws IOException {
  }

  public List<T> returning() {
    return toret;
  }

  public void reuse() {
    toret.clear();
  }

}

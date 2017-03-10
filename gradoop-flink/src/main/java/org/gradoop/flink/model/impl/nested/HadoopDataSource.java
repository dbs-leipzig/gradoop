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

package org.gradoop.flink.model.impl.nested;

import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Defines a HadoopDataSource
 */
public abstract class HadoopDataSource<DefaultRepresentation> implements DataSource {

  private final GradoopFlinkConfig conf;

  public HadoopDataSource(GradoopFlinkConfig conf) {
    this.conf = conf;
  }

  public GradoopFlinkConfig getConf() {
    return conf;
  }

  public abstract DefaultRepresentation getDefaultRepresentation();

}

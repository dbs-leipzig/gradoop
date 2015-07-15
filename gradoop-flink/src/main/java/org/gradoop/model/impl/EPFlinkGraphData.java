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

package org.gradoop.model.impl;

import org.gradoop.model.EPGraphData;

import java.util.Map;

/**
 * POJO for graph data.
 */
public class EPFlinkGraphData extends EPFlinkEntity implements EPGraphData {

  public EPFlinkGraphData() {
  }

  public EPFlinkGraphData(EPFlinkGraphData otherGraphData) {
    super(otherGraphData);
  }

  public EPFlinkGraphData(Long graphID, String label) {
    this(graphID, label, null);
  }

  public EPFlinkGraphData(Long graphID, String label,
    Map<String, Object> properties) {
    super(graphID, label, properties);
  }
}

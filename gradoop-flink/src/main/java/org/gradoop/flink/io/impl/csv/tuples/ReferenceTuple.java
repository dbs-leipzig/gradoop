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

package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Contains all relevant information for a reference: datasource name, domain name,
 * class name and id.
 */
public class ReferenceTuple extends Tuple4<String, String, String, String> {
  /**
   * Empty constructor.
   */
  public ReferenceTuple() {
  }

  /**
   * Valued constructor.
   *
   * @param datasourceName name of the datasource
   * @param domainName name of the domain
   * @param className name of the class
   * @param id the id
   */
  public ReferenceTuple(String datasourceName, String domainName, String className, String id) {
    super(datasourceName, domainName, className, id);
  }

  public String getDatasourceName() {
    return f0;
  }

  public void setDatasourceName(String datasourceName) {
    f0 = datasourceName;
  }

  public String getDomainName() {
    return f1;
  }

  public void setDomainName(String domainName) {
    f1 = domainName;
  }

  public String getClassName() {
    return f2;
  }

  public void setClassName(String className) {
    f2 = className;
  }

  public String getId() {
    return f3;
  }

  public void setId(String id) {
    f3 = id;
  }
}

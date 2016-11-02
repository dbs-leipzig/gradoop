package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Contains all relevant information for a reference: datasource name, domain
 * name, class name and id.
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
  public ReferenceTuple(String datasourceName, String domainName,
    String className, String id) {
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
    f1= domainName;
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

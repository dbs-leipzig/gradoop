package org.gradoop.flink.io.impl.csv.tuples;

import org.apache.flink.api.java.tuple.Tuple4;

/**
 * Created by Stephan on 19.10.16.
 */
public class ReferenceTuple extends Tuple4<String, String, String, String> {

  public ReferenceTuple() {
  }

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

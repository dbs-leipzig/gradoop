package org.gradoop.flink.io.impl.csv.functions;

import org.apache.flink.api.common.functions.CrossFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.gradoop.flink.io.impl.csv.pojos.Csv;

/**
 * Created by galpha on 9/28/16.
 */
public class CreateTuple
  implements CrossFunction<Csv, String, Tuple3<Csv, String, String>> {

  private Tuple3<Csv, String, String> reuse;

  private String domainName;

  public CreateTuple(String domainName){
    this.domainName = domainName;
    this.reuse = new Tuple3<>();
  }

  @Override
  public Tuple3<Csv, String, String> cross(Csv csv, String string) throws
    Exception {
    reuse.f0 = csv;
    reuse.f1 = domainName;
    reuse.f2 = string;
    return reuse;
  }
}

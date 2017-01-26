package org.gradoop.flink.model.impl.operators.utils;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

/**
 * Created by vasistas on 20/01/17.
 */
public interface IWithDependencies {

    Set<String> getDependencies();
    public String toString(boolean withVariableName);
    public String compileWith(HashMap<String,IWithDependencies> infoToExpand, boolean withVariableName);

}

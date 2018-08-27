package org.gradoop.flink.io.impl.rdbms.functions;

import org.apache.flink.api.common.functions.FilterFunction;
import org.gradoop.flink.io.impl.rdbms.metadata.TableToEdge;

public class DirFilter implements FilterFunction<TableToEdge> {

	@Override
	public boolean filter(TableToEdge table) throws Exception {
		return table.isDirectionIndicator();
	}

}

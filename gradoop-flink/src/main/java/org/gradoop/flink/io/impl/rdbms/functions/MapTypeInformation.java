package org.gradoop.flink.io.impl.rdbms.functions;

import java.sql.JDBCType;
import java.sql.SQLInput;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.tuples.RDBMSTable;

public class MapTypeInformation {

	public MapTypeInformation() {
	}

	public static RowTypeInfo getRowTypeInfo(RDBMSTable table) throws ClassNotFoundException {
		int i = 0;
		TypeInformation[] fieldTypes = new TypeInformation[table.getAttributes().size()];
		Iterator it = table.getAttributes().entrySet().iterator();
		while (it.hasNext()) {
			Map.Entry pair = (Entry) it.next();
			JDBCType jdbcType = (JDBCType) pair.getValue();
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(jdbcType);
			i++;
		}
		return new RowTypeInfo(fieldTypes);
	}
}

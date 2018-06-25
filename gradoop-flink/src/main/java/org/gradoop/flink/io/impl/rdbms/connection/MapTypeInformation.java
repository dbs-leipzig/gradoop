package org.gradoop.flink.io.impl.rdbms.connection;

import java.sql.JDBCType;
import java.sql.SQLInput;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.gradoop.flink.io.impl.rdbms.metadata.RDBMSTable;

/**
 * creates RowTypeInfo from given JDBCTypes
 * @author pc
 *
 */
public class MapTypeInformation {

	public static RowTypeInfo getRowTypeInfo(RDBMSTable table) throws ClassNotFoundException {
		int i = 0;
		TypeInformation[] fieldTypes = new TypeInformation[table.getjdbcTypes().size()];
		
		for(JDBCType f : table.getjdbcTypes()){
			fieldTypes[i] = new SQLToBasicTypeMapper().getBasicTypeInfo(f);
			i++;
		}
		return new RowTypeInfo(fieldTypes);
	}
}

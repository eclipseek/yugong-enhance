package com.taobao.yugong.translator;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import org.apache.commons.lang.ObjectUtils;

import java.sql.Types;
import java.util.Date;

/**
 * Created by zhangyq on 2016-11-07.
 */
public class DefaultTranslator  extends AbstractDataTranslator implements DataTranslator {

	public boolean translator(Record record) {

		//// 1. schema/table名不同
		//// record.setSchemaName("test");
		//record.setTableName("yugong_example_mysql");
		//
		//// 2. 字段名字不同
		//ColumnValue nameColumn = record.getColumnByName("name");
		//if (nameColumn != null) {
		//	nameColumn.getColumn().setName("display_name");
		//}
		//
		//// 3. 字段逻辑处理
		//ColumnValue aliasNameColumn = record.getColumnByName("alias_name");
		//if (aliasNameColumn != null) {
		//	StringBuilder displayNameValue = new StringBuilder(64);
		//	displayNameValue.append(ObjectUtils.toString(nameColumn.getValue()))
		//			.append('(')
		//			.append(ObjectUtils.toString(aliasNameColumn.getValue()))
		//			.append(')');
		//	nameColumn.setValue(displayNameValue.toString());
		//}
		//
		//// 4. 字段类型不同
		//ColumnValue amountColumn = record.getColumnByName("amount");
		//if (amountColumn != null) {
		//	amountColumn.getColumn().setType(Types.VARCHAR);
		//	amountColumn.setValue(ObjectUtils.toString(amountColumn.getValue()));
		//}
		//
		//// 5. 源库多一个字段
		//record.removeColumnByName("alias_name");
		//
		//// 6. 目标库多了一个字段
		//ColumnMeta gmtMoveMeta = new ColumnMeta("gmt_move", Types.TIMESTAMP);
		//ColumnValue gmtMoveColumn = new ColumnValue(gmtMoveMeta, new Date());
		//gmtMoveColumn.setCheck(false);// 该字段不做对比
		//record.addColumn(gmtMoveColumn);

		// ColumnValue text_c = record.getColumnByName("text_c");
		// try {
		// text_c.setValue(new String((byte[]) text_c.getValue(), "GBK"));
		// } catch (UnsupportedEncodingException e) {
		// e.printStackTrace();
		// }

		return super.translator(record);
	}
}

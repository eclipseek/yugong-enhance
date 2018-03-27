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
		return super.translator(record);
	}
}

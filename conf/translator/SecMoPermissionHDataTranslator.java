package translator;

import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.AbstractDataTranslator;
import com.taobao.yugong.translator.DataTranslator;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-08
 * Time: 12:57
 */
public class SecMoPermissionHDataTranslator  extends AbstractDataTranslator implements DataTranslator {
    public boolean translator(Record record) {
        // 字段名字不同
        ColumnValue nameColumn = record.getColumnByName("condition");
        if (nameColumn != null) {
            nameColumn.getColumn().setName("condition_mysql");
        }
        return super.translator(record);
    }
}

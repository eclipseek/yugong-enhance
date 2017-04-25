package translator;

import com.taobao.yugong.common.db.meta.ColumnMeta;
import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.AbstractDataTranslator;
import com.taobao.yugong.translator.DataTranslator;
import org.apache.commons.lang.ObjectUtils;

import java.sql.Types;
import java.util.Date;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-07
 * Time: 16:09
 */
public class CacheHitStatisticsDataTranslator extends AbstractDataTranslator implements DataTranslator {
    public boolean translator(Record record) {
        // 字段名字不同
        ColumnValue nameColumn = record.getColumnByName("key");
        if (nameColumn != null) {
            nameColumn.getColumn().setName("key_mysql");
        }
        return super.translator(record);
    }
}

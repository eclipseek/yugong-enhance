package translator;

import com.taobao.yugong.common.db.meta.ColumnValue;
import com.taobao.yugong.common.model.record.Record;
import com.taobao.yugong.translator.AbstractDataTranslator;
import com.taobao.yugong.translator.DataTranslator;

/**
 * created by Intellij IDEA
 * User: yiqiang-zhang
 * Date: 2017-02-10
 * Time: 23:05
 */
// for test
public class TbExtendDataTranslator  extends AbstractDataTranslator implements DataTranslator {
    public boolean translator(Record record) {
        return super.translator(record);
    }
}

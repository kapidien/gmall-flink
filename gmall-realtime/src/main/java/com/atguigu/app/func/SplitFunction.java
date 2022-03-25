package com.atguigu.app.func;

import com.atguigu.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @author shkstart
 * @create 2022-03-23 18:09
 */
@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        List<String> analyze = null;
        try {
            analyze = KeywordUtil.analyze(keyword);
            for (String word : analyze) {
                collect(Row.of(word));
            }
        } catch (IOException e) {
            collect(Row.of(keyword));
        }

    }
}

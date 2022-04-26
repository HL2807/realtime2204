package com.lqs.app.functions;


import com.lqs.utils.KeywordUtil;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

import java.io.IOException;
import java.util.List;

/**
 * @Author lqs
 * @Date 2022年04月22日 18:13:17
 * @Version 1.0.0
 * @ClassName SplitFunction
 * @Describe 搜索切分函数
 */

@FunctionHint(output = @DataTypeHint("ROW<word STRING>"))
public class SplitFunction extends TableFunction<Row> {

    public void eval(String keyword) {

        List<String> list;
        try {
            list= KeywordUtil.splitKeyword(keyword);
            for (String word : list) {
                collect(Row.of(word));
            }
        }catch (IOException e){
            collect(Row.of(keyword));
        }

    }

}

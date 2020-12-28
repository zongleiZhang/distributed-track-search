package com.ada.flinkFunction;

import com.ada.model.GlobalToLocal.GlobalToLocalElem;
import com.ada.model.queryResult.QueryResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HausdorffLocalPF extends ProcessWindowFunction<GlobalToLocalElem, QueryResult, Integer, TimeWindow> {
    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<GlobalToLocalElem> elements,
                        Collector<QueryResult> out) throws Exception {

    }
}

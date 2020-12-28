package com.ada.flinkFunction;

import com.ada.model.DensityToGlobal.DensityToGlobalElem;
import com.ada.model.GlobalToLocal.GlobalToLocalElem;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class HausdorffGlobalPF extends ProcessWindowFunction<DensityToGlobalElem, GlobalToLocalElem, Integer, TimeWindow> {


    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<DensityToGlobalElem> elements,
                        Collector<GlobalToLocalElem> out) throws Exception {

    }
}

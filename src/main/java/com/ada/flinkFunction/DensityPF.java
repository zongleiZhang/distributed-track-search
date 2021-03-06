package com.ada.flinkFunction;

import com.ada.common.Constants;
import com.ada.geometry.TrackPoint;
import com.ada.model.densityToGlobal.D2GElem;
import com.ada.model.densityToGlobal.Density;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

public class DensityPF extends ProcessWindowFunction<TrackPoint, D2GElem, Integer, TimeWindow> {
    private Map<Integer, TrackPoint> tidTPMap;
    private int[][] grids;
    private long count;
    private static long biggestInterval = Constants.logicWindow * Constants.windowSize;
    private int factor;
    private int parallelism;

    private int subTask;

    @Override
    public void process(Integer key,
                        Context context,
                        Iterable<TrackPoint> elements,
                        Collector<D2GElem> out) {
        for (TrackPoint tp : elements) {
            int row = (int) Math.floor(((tp.data[0] - Constants.globalRegion.low.data[0])/(Constants.globalRegion.high.data[0] - Constants.globalRegion.low.data[0]))*(Constants.gridDensity+1.0));
            int col = (int) Math.floor(((tp.data[1] - Constants.globalRegion.low.data[1])/(Constants.globalRegion.high.data[1] - Constants.globalRegion.low.data[1]))*(Constants.gridDensity+1.0));
            grids[row][col]++;
            TrackPoint preTp = tidTPMap.get(tp.TID);
            if (preTp == null){
                tp.key = factor = (factor + 1) & parallelism;
                tidTPMap.put(tp.TID, tp);
                out.collect(tp);
            }else {
                tp.key = preTp.key;
                tidTPMap.replace(tp.TID, tp);
                if (!(Constants.isEqual(tp.data[0], preTp.data[0]) && Constants.isEqual(tp.data[1], preTp.data[1]))){
                    out.collect(tp);
                }
            }
        }
        if (context.window().getStart()%(Constants.densityFre*Constants.windowSize) == 0){
//            StringBuilder sb = new StringBuilder();
//            for (int i = 0; i < subTask; i++)
//                sb.append("\t\t\t");
//            sb.append(subTask).append(":").append(context.window().getStart());
//            System.out.println(sb.toString());
            for (int i = 0; i < Constants.globalPartition; i++) {
                out.collect(new Density(grids, i, subTask));
            }
            grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
            //删除不活跃的轨迹
            if (count%(Constants.logicWindow*5) == 0) {
                tidTPMap.entrySet().removeIf(entry -> context.window().getEnd() - entry.getValue().timestamp > biggestInterval);
            }
        }
        count++;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        tidTPMap = new HashMap<>();
        grids = new int[Constants.gridDensity+1][Constants.gridDensity+1];
        count = 0;
        factor = 0;
        parallelism = Constants.globalPartition-1;
        subTask = getRuntimeContext().getIndexOfThisSubtask();
    }
}

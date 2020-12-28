package com.ada;

import com.ada.common.Constants;
import com.ada.flinkFunction.*;
import com.ada.geometry.TrackPoint;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

public class StreamingJob {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        env.readTextFile("D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101")
                .map(TrackPoint::new)
                .assignTimestampsAndWatermarks(new TrackPointTAndW())

                .keyBy(new RebalanceKeySelector<>(Constants.densityPartition))
                .timeWindow(Time.milliseconds(Constants.windowSize))
                .process(new DensityPF())
                .setParallelism(Constants.densityPartition)

                .keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getD2GKey()))
                .timeWindow(Time.milliseconds(Constants.windowSize))
                .process(new HausdorffGlobalPF())
                .setParallelism(Constants.globalPartition)

                .keyBy(value -> Constants.divideSubTaskKeyMap.get(value.getG2LKey()))
                .timeWindow(Time.milliseconds(Constants.windowSize))
                .process(new HausdorffLocalPF())
                .setParallelism(Constants.dividePartition)
                .print()
        ;
        env.execute("distributed track search");
    }

}

package com.ada;

import com.ada.common.Constants;
import com.ada.flinkFunction.*;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import redis.clients.jedis.Jedis;


public class StreamingJob {

	public static void main(String[] args) throws Exception {
//		Jedis jedis = new Jedis("localhost");
//		jedis.flushDB();
//		jedis.flushAll();
//		jedis.close();

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		env.setParallelism(1);

//		DataStream<TrackPoint> source;
//		Properties properties = new Properties();
//		properties.setProperty("bootstrap.servers", "192.168.100.1:9092");
//		FlinkKafkaConsumer011<TrackPoint> myConsumer =
//				new FlinkKafkaConsumer011<>("trackPoint2000",new AbstractDeserializationSchema<TrackPoint>() {
//					@Override
//					public TrackPoint deserialize(byte[] message) throws IOException {
//						MyPoint.Point myPoint = MyPoint.Point.parseFrom(message);
//						return new TrackPoint(new double[]{myPoint.getLon(), myPoint.getLat()}, myPoint.getTimeStamp(), myPoint.getTID()+1);
//					}
//				}, properties);
//		myConsumer.setStartFromEarliest();
////		myConsumer.setStartFromLatest();  //读最新的
//		source = env.addSource(myConsumer)
//				.setParallelism(Constants.topicPartition);

		String filePath;
		if ("Windows 10".equals(System.getProperty("os.name"))){
			filePath = "D:\\研究生资料\\track_data\\成都滴滴\\Sorted_2D\\XY_20161101";
		}else {
			filePath = "/home/chenliang/data/didi/Cheng_Du/Sorted_2D/XY_20161101";
		}
		env.readTextFile(filePath)
				.map(new MapToTrackPoint())
				.assignTimestampsAndWatermarks(new TrackPointTAndW())

				.keyBy(value -> Constants.densitySubTaskKeyMap.get(value.key))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new DensityPF())
				.setParallelism(Constants.densityPartition)

				.keyBy(value -> Constants.globalSubTaskKeyMap.get(value.getD2GKey()))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new HausdorffGlobalPF())
				.setParallelism(Constants.globalPartition)

				.keyBy(value -> Constants.divideSubTaskKeyMap.get(value.key))
				.timeWindow(Time.milliseconds(Constants.windowSize))
				.process(new HausdorffLocalPF())
				.setParallelism(Constants.dividePartition)

//				.flatMap(new HausdorffOneNodeMF())
//				.setParallelism(1)

				.print()
//				.writeAsText("/home/chenliang/data/zzlDI/" +
//						Constants.logicWindow + "_" +
//						Constants.t + "_" +
//						".txt", FileSystem.WriteMode.OVERWRITE)
//				.setParallelism(1/*Constants.dividePartition*/)
		;
		env.execute("logicWindow: " + Constants.logicWindow +
				" t: " + Constants.t +
				" topK: " + Constants.topK);
	}
}

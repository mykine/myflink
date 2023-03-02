package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.commons.collections.IteratorUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.mortbay.util.ajax.JSON;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class FlinkWindowTest {

    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source-从数据源获取流数据
        // kafka 配置项
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "192.168.10.135:9092");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer",
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        DataStreamSource<String> receiveStreamData
                = env.addSource(new FlinkKafkaConsumer011<String>(
                                        "sensor",
                                        new SimpleStringSchema(),
                                        properties
                                    )
                                );


        //transformation-基于流数据进行转换计算
        DataStream<SensorReading> dataStream = receiveStreamData.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] valueArr = value.split(",");
                return new SensorReading(valueArr[0],
                        Long.valueOf(valueArr[1]),
                        Double.valueOf(valueArr[2]));
            }
        });
//        //滚动时间窗口-增量聚合函数操作-每隔10秒统计这一窗口累加值
//        DataStream<Integer> windowOptData = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(10))
//                .aggregate(new AggregateFunction<SensorReading, Integer, Integer>() {
//                    @Override
//                    public Integer createAccumulator() {
//                        //新窗口累加的初始值
//                        System.out.println("createAccumulator--0");
//                        return 0;
//                    }
//
//                    @Override
//                    public Integer add(SensorReading sensorReading, Integer accumulatorCurrent) {
//                        //累加值的逻辑
//                        System.out.println("add---" + accumulatorCurrent + ",json:" + JSON.toString(sensorReading));
//                        return sensorReading.getTemperature() > 30 ? accumulatorCurrent + 2 : accumulatorCurrent + 1;
//                    }
//
//                    @Override
//                    public Integer getResult(Integer accumulatorCurrent) {
//                        System.out.println("getResult---" + accumulatorCurrent);
//                        return accumulatorCurrent;
//                    }
//
//                    @Override
//                    public Integer merge(Integer a, Integer b) {
//                        return a + b;
//                    }
//                });

        //滚动时间窗口-全窗口聚合函数操作-每隔10秒统计这一窗口内所有数目
       DataStream windowResult2 = dataStream.keyBy("id")
                .timeWindow(Time.seconds(10))
                .apply(new WindowFunction<SensorReading, Tuple3<String, String, Integer>, Tuple, TimeWindow>() {
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
                        String id = tuple.getField(0);
                        String thisEndTime =  new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(window.getEnd());
                        List list = IteratorUtils.toList(input.iterator());
                        Integer count = list.size();
//                        BigDecimal sum = BigDecimal.ZERO;
//                        while(input.iterator().hasNext()){
//                            SensorReading item = input.iterator().next();
//                            sum.add(BigDecimal.valueOf(item.getTemperature()));
//                        }
//                        System.out.println("sum="+sum+",count="+count);
//                        BigDecimal avg = sum.divide(BigDecimal.valueOf(count),2, RoundingMode.HALF_UP);
                        out.collect(new Tuple3<>(id, thisEndTime, count));
                    }
                });


        //sink-输出结果
        windowResult2.print("windowResult2");

        //执行
        env.execute();

    }

    public static class MyMap implements MapFunction<String,String>{

        @Override
        public String map(String value) throws Exception {
            String[] words = value.split(",");
            return words[0]+"的温度是"+words[2];
        }
    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,String>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, String>> out) throws Exception {
            String[] words = value.split(",");

            out.collect(new Tuple2<>(words[0],words[2]));
        }
    }
}

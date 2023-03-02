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

//        //滚动时间窗口-全窗口聚合函数操作-每隔10秒统计这一窗口内所有数目
//       DataStream windowResult2 = dataStream.keyBy("id")
//                .timeWindow(Time.seconds(10))
//                .apply(new WindowFunction<SensorReading, Tuple3<String, String, Integer>, Tuple, TimeWindow>() {
//                    @Override
//                    public void apply(Tuple tuple, TimeWindow window, Iterable<SensorReading> input, Collector<Tuple3<String, String, Integer>> out) throws Exception {
//                        String id = tuple.getField(0);
//                        String thisEndTime =  new SimpleDateFormat("yyyy-MM-dd HH-mm-ss").format(window.getEnd());
//                        List list = IteratorUtils.toList(input.iterator());
//                        Integer count = list.size();
//                        out.collect(new Tuple3<>(id, thisEndTime, count));
//                    }
//                });

        //滑动计数窗口-增量聚合操作-统计最近5个数的平均值
        DataStream windowResult3 = dataStream.keyBy("id")
                .countWindow(10,2)
                        .aggregate(new MyCountWinFunc());


        //sink-输出结果
        windowResult3.print("windowResult3");

        //执行
        env.execute();

    }

    public static class MyCountWinFunc implements AggregateFunction<SensorReading, Tuple2<Double, Integer>, Double>{

        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0,0);
        }

        @Override
        public Tuple2<Double, Integer> add(SensorReading value, Tuple2<Double, Integer> accumulator) {
            return new Tuple2<>(accumulator.f0+value.getTemperature(),accumulator.f1+1);
        }

        @Override
        public Double getResult(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        @Override
        public Tuple2<Double, Integer> merge(Tuple2<Double, Integer> a, Tuple2<Double, Integer> b) {
            return new Tuple2<>(a.f0+b.f0,a.f1+b.f1);
        }
    }
}

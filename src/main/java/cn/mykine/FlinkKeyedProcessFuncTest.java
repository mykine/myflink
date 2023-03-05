package cn.mykine;

import cn.mykine.apitest.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Collections;
import java.util.Properties;

/**
 * 需求：十秒内温度一直升高就报警
 * 解决方法：利用底层api-keyedProcessFunction控制状态和定时器实现监控
 * */
public class FlinkKeyedProcessFuncTest {

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

        DataStream<String> monitorRes = dataStream.keyBy("id")
                .process(new MonitorTempRise(10));

        //sink-输出结果
        monitorRes.print("monitorRes");

        //执行
        env.execute();

    }

    public static class MonitorTempRise extends KeyedProcessFunction<Tuple,SensorReading,String>{

        private Integer interval;//定时器时间长度
        public MonitorTempRise(Integer intervalParam) {
            interval = intervalParam;
        }

        //定义状态
        private ValueState<Double> lastTemp;//最近一次记录的温度状态
        private ValueState<Long> timerTsState;//定时器闹钟时间戳状态

        /**
         * 算子初始化操作
         * */
        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化状态
            lastTemp = getRuntimeContext().getState(
                    new ValueStateDescriptor<Double>("last-temp",Double.class,Double.MIN_VALUE)
            );
            timerTsState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Long>("timer-ts",Long.class)
            );
        }

        @Override
        public void close() throws Exception {
            //任务结束时，清理状态
            if(lastTemp!=null){
                lastTemp.clear();
            }
//            if(timerTsState!=null){
//                timerTsState.clear();
//            }
        }

        //流数据处理逻辑
        @Override
        public void processElement(SensorReading value, Context ctx , Collector<String> out) throws Exception {
            //取出状态中的数据,与当前数据进行比较
            Double lastValue = lastTemp.value();
            if(value.getTemperature() > lastValue && timerTsState.value()==null){
                //数据上升，定时器为空时，构造定时器，开启定时闹钟功能
                //设置闹钟触发的时间点并注册
               Long ts = ctx.timerService().currentProcessingTime()+interval*1000L;
               ctx.timerService().registerProcessingTimeTimer(ts);
               timerTsState.update(ts);
            }

            if( value.getTemperature() <= lastValue && timerTsState.value() != null ){
                //出现温度下降，就释放此次定时监控
                ctx.timerService().deleteProcessingTimeTimer(timerTsState.value());
                timerTsState.clear();
            }
            lastTemp.update(value.getTemperature());
        }


        /**
         * 闹钟响了-定时器触发逻辑
         * */
        @Override
        public void onTimer(long timestamp,
                            OnTimerContext ctx,
                            Collector<String> out
        ) throws Exception {
            //输出监控到的信息
            out.collect("传感器" + ctx.getCurrentKey().getField(0) + "温度值连续" + interval + "s上升");
            //此次监控完毕，清理此次的定时器
            timerTsState.clear();
        }

    }

}

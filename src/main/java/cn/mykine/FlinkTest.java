package cn.mykine;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkTest {
    public static void main(String[] args) throws Exception {
        //env-创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //source-从数据源获取流数据
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        host = StringUtils.isBlank(host) ? "192.168.10.98" : host;
        String portStr = parameterTool.get("port");
        int port = StringUtils.isBlank(portStr) ? 7777 : Integer.valueOf(portStr);
        DataStreamSource<String> receiveStreamData
                = env.socketTextStream(host,port)
                .setParallelism(1);

        //transformation-基于流数据进行转换计算
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = receiveStreamData
                .flatMap(new MyFlatMap())
                .setParallelism(2)
                .keyBy(0)
                .sum(1);

        //sink-输出结果
        result.print();

        //执行
        env.execute();

    }

    public static class MyFlatMap implements FlatMapFunction<String, Tuple2<String,Integer>>{

        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] words = value.split(" ");
            for (String word:words
                 ) {
                out.collect(new Tuple2<>(word,1));
            }
        }
    }
}

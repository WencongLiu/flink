package org.apache.flink.api.datastream;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.WindowAssigner;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.Collector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class WIndowOperatorTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        DataStreamSource<String> source =
                executionEnvironment.fromCollection(
                        new EndOfStreamWindows.OneHundredAndTenTupleSource(), String.class);

        SingleOutputStreamOperator<Tuple2<String, Integer>> map =
                source.map(
                        new MapFunction<String, Tuple2<String, Integer>>() {
                            @Override
                            public Tuple2<String, Integer> map(String value) throws Exception {
                                String[] words = value.split(" ");
                                return Tuple2.of(words[0], Integer.valueOf(words[1]));
                            }
                        });
        map.setParallelism(10);
        KeyedStream<Tuple2<String, Integer>, String> keyedStream =
                map.keyBy((KeySelector<Tuple2<String, Integer>, String>) element -> element.f0);
        SingleOutputStreamOperator<Tuple2<String, Integer>> apply =
                keyedStream
                        .window(new EndOfStreamWindows())
                        .apply(
                                new WindowFunction<
                                        Tuple2<String, Integer>,
                                        Tuple2<String, Integer>,
                                        String,
                                        TimeWindow>() {
                                    @Override
                                    public void apply(
                                            String s,
                                            TimeWindow window,
                                            Iterable<Tuple2<String, Integer>> inputData,
                                            Collector<Tuple2<String, Integer>> out) {
                                        Set<String> uniqueStringKeys = new HashSet<>();
                                        int sum = 0;
                                        for (Tuple2<String, Integer> data : inputData) {
                                            uniqueStringKeys.add(data.f0);
                                            sum += data.f1;
                                        }
                                        String key =
                                                uniqueStringKeys.size() + " " + uniqueStringKeys;
                                        out.collect(Tuple2.of(key, sum));
                                    }
                                });
        apply.setParallelism(20);
        apply.print();
        CloseableIterator<Tuple2<String, Integer>> jobResult = apply.executeAndCollect();
        while (jobResult.hasNext()) System.out.println(jobResult.next());
    }

    static class EndOfStreamWindows extends WindowAssigner<Object, TimeWindow> {
        private static final long serialVersionUID = 1L;

        private static final EndOfStreamWindows INSTANCE = new EndOfStreamWindows();

        private static final TimeWindow TIME_WINDOW_INSTANCE =
                new TimeWindow(Long.MIN_VALUE, Long.MAX_VALUE);

        private EndOfStreamWindows() {}

        public static EndOfStreamWindows get() {
            return INSTANCE;
        }

        @Override
        public Collection<TimeWindow> assignWindows(
                Object element, long timestamp, WindowAssignerContext context) {
            return Collections.singletonList(TIME_WINDOW_INSTANCE);
        }

        @Override
        public Trigger<Object, TimeWindow> getDefaultTrigger(StreamExecutionEnvironment env) {
            return new EndOfStreamTrigger();
        }

        @Override
        public String toString() {
            return "EndOfStreamWindows()";
        }

        @Override
        public TypeSerializer<TimeWindow> getWindowSerializer(ExecutionConfig executionConfig) {
            return new TimeWindow.Serializer();
        }

        @Override
        public boolean isEventTime() {
            return true;
        }

        /**
         * Default {@link Trigger} of {@link EndOfStreamWindows} that fires iff the input stream
         * reaches EOF.
         */
        static class EndOfStreamTrigger extends Trigger<Object, TimeWindow> {
            @Override
            public TriggerResult onElement(
                    Object element, long timestamp, TimeWindow window, TriggerContext ctx)
                    throws Exception {
                return TriggerResult.CONTINUE;
            }

            @Override
            public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) {
                return time == window.maxTimestamp() ? TriggerResult.FIRE : TriggerResult.CONTINUE;
            }

            @Override
            public void clear(TimeWindow window, TriggerContext ctx) throws Exception {}

            @Override
            public TriggerResult onProcessingTime(
                    long time, TimeWindow window, TriggerContext ctx) {
                return TriggerResult.CONTINUE;
            }
        }

        static class OneHundredAndTenTupleSource implements Iterator<String>, Serializable {

            private int currentPosition = 0;

            private final List<Integer> allElements = new ArrayList<>();

            public OneHundredAndTenTupleSource() {
                for (int index = 0; index < 100; ++index) {
                    for (int round = 0; round < 10; ++round) {
                        allElements.add(index);
                    }
                }
            }

            @Override
            public boolean hasNext() {
                return currentPosition < 1000;
            }

            @Override
            public String next() {
                return allElements.get(currentPosition++) + " 1";
            }
        }
    }
}

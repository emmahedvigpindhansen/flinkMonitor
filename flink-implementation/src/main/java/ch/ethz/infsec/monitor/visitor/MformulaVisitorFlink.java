package ch.ethz.infsec.monitor.visitor;
import ch.ethz.infsec.Main;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.VariableID;
import javassist.compiler.ast.Variable;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.slicer.*;
import scala.PartialFunction;
import scala.collection.JavaConverters;


public class MformulaVisitorFlink implements MformulaVisitor<DataStream<PipelineEvent>> {

    HashMap<String, OutputTag<Fact>> hmap;
    SingleOutputStreamOperator<Fact> mainDataStream;

    public MformulaVisitorFlink(HashMap<String, OutputTag<Fact>> hmap, SingleOutputStreamOperator<Fact> mainDataStream){
        this.hmap = hmap;
        this.mainDataStream = mainDataStream;
    }

    public DataStream<PipelineEvent> visit(MPred pred) {
        OutputTag<Fact> factStream = this.hmap.get(pred.getPredName());
        return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(1);
        // return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MAnd f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);

        // get common keys
        List<VariableID> commonKeys = f.keys;

        // flatmap to create stream which has key "keys" and duplicate terminators
        // ...

        int numKeys = 3;
        scala.collection.mutable.HashMap<Object, Object> map = ColissionlessKeyGenerator.getMapping(numKeys);
        Map<Object, Object> mapping = JavaConverters.mapAsJavaMap(map);
        System.out.println(mapping);

        // duplicate terminators wrt. parallelism
        DataStream<PipelineEvent> input1duplicate = input1.flatMap(new FlatMapFunction<PipelineEvent, PipelineEvent>() {
            @Override
            public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
                if (!event.isPresent()) {
                    event.terminatorKey = (int) mapping.get(0);
                    PipelineEvent event2 = new PipelineEvent(event.getTimestamp(), event.getTimepoint(), true, event.get());
                    event2.terminatorKey = (int) mapping.get(1);
                    PipelineEvent event3 = new PipelineEvent(event.getTimestamp(), event.getTimepoint(), true, event.get());
                    event3.terminatorKey = (int) mapping.get(2);
                    out.collect(event); out.collect(event2); out.collect(event3);
                } else {
                    event.terminatorKey = (int) mapping.get(0);
                    out.collect(event);
                }
            }
        }).setParallelism(1);
        input1duplicate.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_and_input1duplicate", FileSystem.WriteMode.OVERWRITE);

        /*DataStream<PipelineEvent> input2duplicate = input2.flatMap(new FlatMapFunction<PipelineEvent, PipelineEvent>() {
            @Override
            public void flatMap(PipelineEvent event, Collector<PipelineEvent> out) throws Exception {
                if (!event.isPresent()) {
                    out.collect(event); out.collect(event); out.collect(event);
                } else {
                    event.key = commonKeys;
                    out.collect(event);
                }
            }
        });*/

        KeyedStream<PipelineEvent, Integer> input1Keyed = input1duplicate
                .keyBy(new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.terminatorKey;
                    }
                });
        input1Keyed.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_and", FileSystem.WriteMode.OVERWRITE);
        // flatmap to give each event the common keys to key by

        /*ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreamsKeyed = input1
                .keyBy(new KeySelector<PipelineEvent, List<VariableID>>() {
                    @Override
                    public List<VariableID> getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                        .connect(input2.keyBy(new KeySelector<PipelineEvent, List<VariableID>>() {
                    @Override
                    public List<VariableID> getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                }));*/

        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MExists f) {
        DataStream<PipelineEvent> input = f.subFormula.accept(this);
        return input.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MNext f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MOr f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
        //flatMap here will be interpreted as a coflatmap because the argumetn it receives is an Or,
        //which is a binary operator so it receives a coflatmap. This will apply flatMap1 or flatMap2 depending
        //on the input stream
    }

    public DataStream<PipelineEvent> visit(MPrev f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MSince f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MUntil f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    @Override
    public DataStream<PipelineEvent> visit(MOnce f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f).setParallelism(Main.numberProcessors);
    }

    @Override
    public DataStream<PipelineEvent> visit(MEventually f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        return input.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MRel f) {
        OutputTag<Fact> factStream = this.hmap.get("0Terminator");
        return this.mainDataStream.getSideOutput(factStream).flatMap(f).setParallelism(Main.numberProcessors);
    }

}

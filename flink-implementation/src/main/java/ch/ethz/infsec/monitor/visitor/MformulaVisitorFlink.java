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
        pred.setNumberProcessors(1);
        return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(1);
        // return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MAnd f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);

        f.setNumberProcessors(Main.numberProcessors);

        // get common keys
        // List<VariableID> commonKeys = f.keys;

        // get collisionless keys
        scala.collection.mutable.HashMap<Object, Object> map = ColissionlessKeyGenerator
                .getMapping(Main.numberProcessors);
        Map<Object, Object> mapping = JavaConverters.mapAsJavaMap(map);
        System.out.println(mapping);

        // flatmap to duplicate terminators and set key of events
        DataStream<PipelineEvent> input1duplicated = input1
                .flatMap(new DuplicateTerminators(Main.numberProcessors, mapping))
                .setParallelism(1);
        // input1duplicated.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_and_input1duplicate", FileSystem.WriteMode.OVERWRITE);
        DataStream<PipelineEvent> input2duplicated = input2
                .flatMap(new DuplicateTerminators(Main.numberProcessors, mapping))
                .setParallelism(1);
        // key stream
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreamsKeyed = input1
                .keyBy(new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                .connect(
                        input2.keyBy(new KeySelector<PipelineEvent, Integer>() {
                            @Override
                            public Integer getKey(PipelineEvent event) throws Exception {
                                return event.key;
                            }
                }));
        // see result from keyed stream
        KeyedStream<PipelineEvent, Integer> input1Keyed = input1duplicated
                .keyBy(new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                });
        input1Keyed.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_and_1", FileSystem.WriteMode.OVERWRITE);
        KeyedStream<PipelineEvent, Integer> input2Keyed = input2duplicated
                .keyBy(new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                });
        input2Keyed.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_and_2", FileSystem.WriteMode.OVERWRITE);

        // ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
        return connectedStreamsKeyed.flatMap(f).setParallelism(Main.numberProcessors);
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

        f.setNumberProcessors(Main.numberProcessors);

        // get collisionless keys
        scala.collection.mutable.HashMap<Object, Object> map = ColissionlessKeyGenerator
                .getMapping(Main.numberProcessors);
        Map<Object, Object> mapping = JavaConverters.mapAsJavaMap(map);
        System.out.println(mapping);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, mapping))
                .setParallelism(1);
        // key stream
        KeyedStream<PipelineEvent, Integer> inputKeyed = inputduplicated
                .keyBy(new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                });
        inputKeyed.flatMap(f).setParallelism(Main.numberProcessors).writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_once", FileSystem.WriteMode.OVERWRITE);
        return inputKeyed.flatMap(f).setParallelism(Main.numberProcessors);
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

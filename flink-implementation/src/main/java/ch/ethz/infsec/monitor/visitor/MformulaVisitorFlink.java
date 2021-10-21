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

import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;
import ch.ethz.infsec.slicer.*;
import scala.PartialFunction;

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
        List<VariableID> keys = f.keys;

        // flatmap to create stream which has key "keys" and duplicate terminators
        // ...

        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreamsKeyed = input1
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
                }));

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

package ch.ethz.infsec.monitor.visitor;
import ch.ethz.infsec.Main;
import ch.ethz.infsec.monitor.Fact;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.OutputTag;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

import ch.ethz.infsec.monitor.*;
import ch.ethz.infsec.util.*;
import static ch.ethz.infsec.formula.JavaGenFormula.convert;
import ch.ethz.infsec.monitor.visitor.MOnceEvaluator;

public class MformulaVisitorFlink implements MformulaVisitor<DataStream<PipelineEvent>> {

    HashMap<String, OutputTag<Fact>> hmap;
    SingleOutputStreamOperator<Fact> mainDataStream;
    DataStream<PipelineEvent> terminators;

    public MformulaVisitorFlink(HashMap<String, OutputTag<Fact>> hmap, SingleOutputStreamOperator<Fact> mainDataStream){
        this.hmap = hmap;
        this.mainDataStream = mainDataStream;
        terminators = this.mainDataStream.getSideOutput(this.hmap.get("0Terminator")).map(new MapFunction<Fact, PipelineEvent>() {
            @Override
            public PipelineEvent map(Fact fact) throws Exception {
                return PipelineEvent.terminator(fact.getTimestamp(),fact.getTimepoint());
            }
        });
    }

    public DataStream<PipelineEvent> visit(MPred pred) {
        OutputTag<Fact> factStream = this.hmap.get(pred.getPredName());
        // return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(1);
        return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MAnd f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1.connect(input2);
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
        System.out.println("visit MOnce");
        DataStream<PipelineEvent> input = f.formula.accept(this);

        // remove terminators from stream
        DataStream<PipelineEvent> events = input.filter(new FilterFunction<PipelineEvent>() {
            @Override
            public boolean filter(PipelineEvent event) throws Exception {
                return (event.isPresent());
            }
        });
        DataStream<PipelineEvent> eventsRandomKey = events.map(new MapFunction<PipelineEvent, PipelineEvent>() {
            @Override
            public PipelineEvent map(PipelineEvent event) throws Exception {
                event.key = ThreadLocalRandom.current().nextLong(1,3);
                return event;
            }
        });
        // key event stream - well not really
        KeyedStream<PipelineEvent, Long> eventsKeyed = eventsRandomKey
                .keyBy((KeySelector<PipelineEvent, Long>) event -> event.key);
        // prepare broadcast state (name, key type, value type)
        // here only a single terminator stored at a time (key type void)
        MapStateDescriptor<Void, PipelineEvent> bcStateDescriptor =
                new MapStateDescriptor<>(
                        "terminators", Types.VOID, Types.GENERIC(PipelineEvent.class));
        BroadcastStream<PipelineEvent> bcedTerminators = this.terminators.broadcast(bcStateDescriptor);
        DataStream<PipelineEvent> result = eventsKeyed
                .connect(bcedTerminators)
                .process(new MOnceEvaluator());

        result.writeAsText("/Users/emmahedvigpindhansen/Desktop/BA/my_project/flinkMonitor/res_once", FileSystem.WriteMode.OVERWRITE);

        // return input.flatMap(f).setParallelism(Main.numberProcessors).broadcast(); // all events to both processors
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

package ch.ethz.infsec.monitor.visitor;
import ch.ethz.infsec.Main;
import ch.ethz.infsec.monitor.Fact;
import ch.ethz.infsec.policy.VariableID;
import javassist.compiler.ast.Variable;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
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

    public DataStream<PipelineEvent> visit(MPred f) {
        OutputTag<Fact> factStream = this.hmap.get(f.getPredName());
        f.setNumberProcessors(1);
        return this.mainDataStream.getSideOutput(factStream).flatMap(f).setParallelism(1);
        // return this.mainDataStream.getSideOutput(factStream).flatMap(pred).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MAnd f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);

        // as this MFormula is binary (each operator receives 2 streams), we need to multiply numberProcessors with 2
        // so that next MFormula knows how many terminators to wait for
        f.setNumberProcessors(Main.numberProcessors * 2);

        // get common keys
        // List<VariableID> commonKeys = f.keys;
        // what if no common keys?

        // flatmap to duplicate terminators and set key of events
        DataStream<PipelineEvent> input1duplicated = input1
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        DataStream<PipelineEvent> input2duplicated = input2
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        // partition data
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1duplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }}, new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                .connect(
                        input2duplicated.partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }}, new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                }));
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MExists f) {
        DataStream<PipelineEvent> input = f.subFormula.accept(this);
        f.setNumberProcessors(Main.numberProcessors);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        DataStream<PipelineEvent> partitioned = inputduplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }
        }, new KeySelector<PipelineEvent, Integer>() {
            @Override
            public Integer getKey(PipelineEvent event) throws Exception {
                return event.key;
            }
        });
        return partitioned.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MNext f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        f.setNumberProcessors(Main.numberProcessors);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        DataStream<PipelineEvent> partitioned = inputduplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }
        }, new KeySelector<PipelineEvent, Integer>() {
            @Override
            public Integer getKey(PipelineEvent event) throws Exception {
                return event.key;
            }
        });
        return partitioned.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MOr f) {
        DataStream<PipelineEvent> input1 = f.op1.accept(this);
        DataStream<PipelineEvent> input2 = f.op2.accept(this);

        // as this MFormula is binary (each operator receives 2 streams), we need to multiply numberProcessors with 2
        // so that next MFormula knows how many terminators to wait for
        f.setNumberProcessors(Main.numberProcessors * 2);

        // get common keys
        // List<VariableID> commonKeys = f.keys;

        // flatmap to duplicate terminators and set key of events
        DataStream<PipelineEvent> input1duplicated = input1
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        DataStream<PipelineEvent> input2duplicated = input2
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1duplicated.partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }}, new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                .connect(
                        input2duplicated.partitionCustom(new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % numPartitions;
                            }}, new KeySelector<PipelineEvent, Integer>() {
                            @Override
                            public Integer getKey(PipelineEvent event) throws Exception {
                                return event.key;
                            }
                        }));
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MPrev f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        f.setNumberProcessors(Main.numberProcessors);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        DataStream<PipelineEvent> partitioned = inputduplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }
        }, new KeySelector<PipelineEvent, Integer>() {
            @Override
            public Integer getKey(PipelineEvent event) throws Exception {
                return event.key;
            }
        });
        return partitioned.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MSince f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);

        // as this MFormula is binary (each operator receives 2 streams), we need to multiply numberProcessors with 2
        // so that next MFormula knows how many terminators to wait for
        f.setNumberProcessors(Main.numberProcessors * 2);

        // get common keys
        // List<VariableID> commonKeys = f.keys;

        // flatmap to duplicate terminators and set key of events
        DataStream<PipelineEvent> input1duplicated = input1
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        DataStream<PipelineEvent> input2duplicated = input2
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        // partition data
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1duplicated.partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }}, new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                .connect(
                        input2duplicated.partitionCustom(new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % numPartitions;
                            }}, new KeySelector<PipelineEvent, Integer>() {
                            @Override
                            public Integer getKey(PipelineEvent event) throws Exception {
                                return event.key;
                            }
                        }));
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MUntil f) {
        DataStream<PipelineEvent> input1 = f.formula1.accept(this);
        DataStream<PipelineEvent> input2 = f.formula2.accept(this);

        // as this MFormula is binary (each operator receives 2 streams), we need to multiply numberProcessors with 2
        // so that next MFormula knows how many terminators to wait for
        f.setNumberProcessors(Main.numberProcessors * 2);

        // get common keys
        // List<VariableID> commonKeys = f.keys;

        // flatmap to duplicate terminators and set key of events
        DataStream<PipelineEvent> input1duplicated = input1
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        DataStream<PipelineEvent> input2duplicated = input2
                .flatMap(new DuplicateTerminators(Main.numberProcessors, true))
                .setParallelism(1);
        // partition data
        ConnectedStreams<PipelineEvent, PipelineEvent> connectedStreams = input1duplicated.partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;
                    }}, new KeySelector<PipelineEvent, Integer>() {
                    @Override
                    public Integer getKey(PipelineEvent event) throws Exception {
                        return event.key;
                    }
                })
                .connect(
                        input2duplicated.partitionCustom(new Partitioner<Integer>() {
                            @Override
                            public int partition(Integer key, int numPartitions) {
                                return key % numPartitions;
                            }}, new KeySelector<PipelineEvent, Integer>() {
                            @Override
                            public Integer getKey(PipelineEvent event) throws Exception {
                                return event.key;
                            }
                        }));
        return connectedStreams.flatMap(f).setParallelism(Main.numberProcessors);
    }

    @Override
    public DataStream<PipelineEvent> visit(MOnce f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        f.setNumberProcessors(Main.numberProcessors);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        DataStream<PipelineEvent> partitioned = inputduplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }
        }, new KeySelector<PipelineEvent, Integer>() {
            @Override
            public Integer getKey(PipelineEvent event) throws Exception {
                return event.key;
            }
        });
        return partitioned.flatMap(f).setParallelism(Main.numberProcessors);
    }

    @Override
    public DataStream<PipelineEvent> visit(MEventually f) {
        DataStream<PipelineEvent> input = f.formula.accept(this);
        f.setNumberProcessors(Main.numberProcessors);
        // duplicate terminators
        DataStream<PipelineEvent> inputduplicated = input
                .flatMap(new DuplicateTerminators(Main.numberProcessors, false))
                .setParallelism(1);
        // partition data
        DataStream<PipelineEvent> partitioned = inputduplicated.partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer key, int numPartitions) {
                return key % numPartitions;
            }
        }, new KeySelector<PipelineEvent, Integer>() {
            @Override
            public Integer getKey(PipelineEvent event) throws Exception {
                return event.key;
            }
        });
        return partitioned.flatMap(f).setParallelism(Main.numberProcessors);
    }

    public DataStream<PipelineEvent> visit(MRel f) {
        OutputTag<Fact> factStream = this.hmap.get("0Terminator");
        f.setNumberProcessors(1);
        // return this.mainDataStream.getSideOutput(factStream).flatMap(f).setParallelism(Main.numberProcessors);
        return this.mainDataStream.getSideOutput(factStream).flatMap(f).setParallelism(1);
    }

}

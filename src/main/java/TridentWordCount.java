package storm.starter.trident;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.testing.FixedBatchSpout;
import storm.trident.tuple.TridentTuple;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TridentWordCount {

    public static class SplitWords extends BaseFunction {
        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            String text = tuple.getString(0);
            Pattern p = Pattern.compile("(\\p{Alpha}){2,}");
            Matcher m = p.matcher(text);
            while (m.find()) {
                collector.emit(new Values(m.group().toLowerCase()));
            }
        }
    }

    private static class PrintFilter extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            System.out.println(String.format("--- %s:%d ---", tuple.getString(0), tuple.getLong(1)));
            return false;
        }
    }

    public static void main(String[] args) throws Exception {
        FixedBatchSpout spout = new FixedBatchSpout(new Fields("sentence"), 3,
                new Values("Les petits oiseaux chantent."),
                new Values("Les petits oiseaux chantent joyeusement."));
        spout.setCycle(false);

        TridentTopology topology = new TridentTopology();
        topology.newStream("sentenceStream", spout)
                .each(new Fields("sentence"), new SplitWords(), new Fields("word"))
                .groupBy(new Fields("word"))
                .aggregate(new Count(), new Fields("count"))
                .each(new Fields("word", "count"), new PrintFilter());

        Config conf = new Config();
        conf.setDebug(false);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            // Submit to remote Storm cluster
            StormSubmitter.submitTopology(args[0], conf, topology.build());
        } else {
            // Start a local Storm cluster for testing
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("trident-word-count", conf, topology.build());
            Utils.sleep(10000);
            cluster.killTopology("trident-word-count");
            cluster.shutdown();
        }
    }
}
package storm.starter;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class StormWordCount {

    private static class SentenceSpout extends BaseRichSpout {
        private SpoutOutputCollector collector;
        private Iterator<String> data;

        @Override
        public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
            this.collector = collector;

            List<String> sentences = new ArrayList<String>();
            sentences.add("Les petits oiseaux chantent.");
            sentences.add("Les petits oiseaux chantent joyeusement.");
            this.data = sentences.iterator();
        }

        @Override
        public void nextTuple() {
            Utils.sleep(100);

            if (data.hasNext()) {
                String sentence = data.next();
                collector.emit(new Values(sentence));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }

    }

    public static class SplitSentence extends BaseBasicBolt {

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String text = tuple.getString(0);
            Pattern p = Pattern.compile("(\\p{Alpha}){2,}");
            Matcher m = p.matcher(text);
            while (m.find()) {
                collector.emit(new Values(m.group().toLowerCase()));
            }
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }
    }

    public static class WordCount extends BaseBasicBolt {
        private Map<String, Integer> counts = new HashMap<String, Integer>();

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
            String word = tuple.getString(0);
            Integer count = counts.get(word);
            if (count == null) {
                count = 0;
            }
            count++;
            counts.put(word, count);
            collector.emit(new Values(word, count));
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word", "count"));
        }
    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("sentenceSpout", new SentenceSpout());
        builder.setBolt("splitBolt", new SplitSentence()).shuffleGrouping("sentenceSpout");
        builder.setBolt("countBolt", new WordCount()).fieldsGrouping("splitBolt", new Fields("word"));

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(2);
            // Submit to remote Storm cluster
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
        } else {
            // Start a local Storm cluster for testing
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("storm-topo-exclaim", conf, builder.createTopology());
            Utils.sleep(20000);
            cluster.killTopology("storm-topo-exclaim");
            cluster.shutdown();
        }
    }
}
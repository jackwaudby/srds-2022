import action.CommitReceivedAction;
import action.NodeTimeoutAction;
import action.PrepareReceivedAckAction;
import action.PrepareReceivedAction;
import action.TransactionCompletionAction;
import event.*;
import org.apache.log4j.Logger;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import state.Cluster;
import utils.*;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

public class Main implements Callable<Integer> {
    private final static Logger LOGGER = Logger.getLogger(Main.class.getName());

    @Option(names = {"-c", "--clusterSize"}, description = "Cluster size")
    private int cluster = 64;

    @Option(names = {"-e", "--epochTimeout"}, description = "Epoch timeout (ms)")
    private int epochTimeout = 10;

    @Option(names = {"-n", "--networkDelay"}, description = "Average network delay (ms)")
    private double networkDelayRate = 0.5;

    @Option(names = {"-st", "--shortTransactionRate"}, description = "Average short transaction service rate (ms)")
    private double shortTransactionServiceRate = 1;

    @Option(names = {"-lt", "--longTransactionRate"}, description = "Average long transaction service rate (ms)")
    private double longTransactionServiceRate = 1000;

    @Option(names = {"-s", "--fixSeed"}, description = "Fix seed")
    private String fixSeed = "false";

    @Option(names = {"-d", "--duration"}, description = "Simulation duration (secs)")
    private double timeLimit = 3600;

    @Option(names = {"-pl", "--propLongTransaction"}, description = "Proportion of long transactions")
    private double propLongTransactions = 0.1;

    @Option(names = {"-pd", "--propDistributedTransaction"}, description = "Proportion of distributed transactions")
    private double propDistributedTransactions = 0.1;

    @Override
    public Integer call() {
        // config
        var config = Config.getInstance();
        config.setClusterSize(cluster);
        config.setEpochTimeout(epochTimeout);
        config.setCommitOperationRate(networkDelayRate);
        config.setShortTransactionServiceRate(shortTransactionServiceRate);
        config.setLongTransactionServiceRate(longTransactionServiceRate);
        config.setPropLongTransactions(propLongTransactions);
        config.setPropDistributedTransaction(propDistributedTransactions);
        config.setFixSeed(Boolean.parseBoolean(fixSeed));

        // global variables
        var rand = Rand.getInstance();
        var eventList = EventList.getInstance();
        var metrics = Metrics.getInstance();
        var cluster = Cluster.getInstance();

        // initial transaction events and timeouts
        var clusterSize = config.getClusterSize();
        for (int nodeId = 0; nodeId < clusterSize; nodeId++) {
            var transactionEvent = new TransactionCompletionEvent(rand.generateShortTransactionServiceTime(), EventType.TRANSACTION_COMPLETED, nodeId);
            eventList.addEvent(transactionEvent);

            var initEventTime = rand.generateNextEpochTimeout();
            var epochEvent = new NodeTimeoutEvent(initEventTime, EventType.NODE_EPOCH_TIMEOUT, nodeId, 0);
            eventList.addEvent(epochEvent);
        }

        // run simulation
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        LocalDateTime now = LocalDateTime.now();

        LOGGER.info("--------------------");
        LOGGER.info(String.format("Starting simulation at %s", dtf.format(now)));
        LOGGER.info("Simulation time (secs): " + String.format("%.5f", timeLimit));
        LOGGER.info("Configuration: " + Config.getInstance());
        var start = System.currentTimeMillis();
        Clock.getInstance().setSimStartTime(start);
        LOGGER.info("Simulating....");
        runSimulation(timeLimit, config, rand, eventList);
        var end = System.currentTimeMillis();
        metrics.getSummary();
        var realTime = (end - start) / 1000.0;
        LOGGER.info("Real time (secs): " + String.format("%.5f", realTime));
        LOGGER.info("Simulation completed!");
        LOGGER.info("--------------------");
        LOGGER.info("");

        WriteOutResults.writeOutResults(config, metrics, realTime, timeLimit);

        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Main()).execute(args);
        System.exit(exitCode);
    }

    private static void runSimulation(double timeLimit, Config config, Rand rand, EventList eventList) {
        var cluster = Cluster.getInstance();
        var clock = Clock.getInstance();
        var metrics = Metrics.getInstance();

        while (clock.getClock() < timeLimit) {
            var nextEvent = eventList.getNextEvent();
            clock.setClock(nextEvent.getEventTime());

            LOGGER.debug("Event: " + nextEvent.getEventType() + " at " + String.format("%.2f (ms)", nextEvent.getEventTime() * 1000.0));
            LOGGER.debug("Action(s): ");

            var eventType = nextEvent.getEventType();

            switch (eventType) {
                case TRANSACTION_COMPLETED ->
                        TransactionCompletionAction.execute((TransactionCompletionEvent) nextEvent, cluster, eventList, rand);
                case NODE_EPOCH_TIMEOUT ->
                        NodeTimeoutAction.timeout((NodeTimeoutEvent) nextEvent, cluster, rand, eventList);
                case PREPARE_RECEIVED ->
                        PrepareReceivedAction.prepareReceived((PrepareReceivedEvent) nextEvent, cluster, rand, eventList);
                case PREPARE_ACK_RECEIVED ->
                        PrepareReceivedAckAction.prepareAckReceived((PrepareAckReceivedEvent) nextEvent, cluster, rand, eventList);
                case COMMIT_RECEIVED ->
                        CommitReceivedAction.commitReceived((CommitReceivedEvent) nextEvent, cluster, config, eventList, rand, metrics);
            }

            LOGGER.debug("");
        }
        cluster.setStop(clock.getClock());
    }
}

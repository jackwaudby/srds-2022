import action.CommitOperationAction;
import action.EpochTimeoutAction;
import action.ReadyToCommitAction;
import action.TransactionAction;
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

    @Option(names = {"-n", "--cluster"}, description = "Cluster size")
    private int cluster = 64;

    @Option(names = {"-a", "--epoch"}, description = "Epoch timeout (ms)")
    private int epochTimeout = 10;

    @Option(names = {"-b", "--commit"}, description = "Average commit operation rate (ms)")
    private double commitOperationRate = 1.7;

    @Option(names = {"-st", "--shortTransactionRate"}, description = "Average short transaction service rate (ms)")
    private double shortTransactionServiceRate = 1;

    @Option(names = {"-lt", "--longTransactionRate"}, description = "Average long transaction service rate (ms)")
    private double longTransactionServiceRate = 10;

    @Option(names = {"-s", "--seed"}, description = "Fix seed")
    private String fixSeed = "false";

    @Option(names = {"-d", "--duration"}, description = "Simulation duration (secs)")
    private double timeLimit = 3600;

    @Option(names = {"-pl", "--propLongTransaction"}, description = "Proportion of long transactions")
    private double propLongTransactions = 0.1;
    @Override
    public Integer call() {
        // config
        var config = Config.getInstance();
        config.setClusterSize(cluster);
        config.setEpochTimeout(epochTimeout);
        config.setCommitOperationRate(commitOperationRate);
        config.setShortTransactionServiceRate(shortTransactionServiceRate);
        config.setLongTransactionServiceRate(longTransactionServiceRate);
        config.setPropLongTransactions(propLongTransactions);
        config.setFixSeed(Boolean.parseBoolean(fixSeed));

        // global variables
        var rand = Rand.getInstance();
        var eventList = EventList.getInstance();
        var metrics = Metrics.getInstance();
        var cluster = Cluster.getInstance();

        // coordinator node initialisation event
        var initEventTime = rand.generateNextEpochTimeout();
        var epochEvent = new EpochTimeoutEvent(initEventTime, EventType.EPOCH_TIMEOUT, 0);
        eventList.addEvent(epochEvent);

        // initial transaction events
        var clusterSize = config.getClusterSize();
        for (int i = 0; i < clusterSize; i++) {
            var transactionEvent = new TransactionEvent(rand.generateShortTransactionServiceTime(), EventType.TRANSACTION_COMPLETED, i, 0);
            eventList.addEvent(transactionEvent);
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
        metrics.getSummary(cluster);
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
            AbstractEvent nextEvent = eventList.getNextEvent();
            clock.setClock(nextEvent.getEventTime());

            var eventType = nextEvent.getEventType();

            switch (eventType) {
                case TRANSACTION_COMPLETED ->
                        TransactionAction.execute((TransactionEvent) nextEvent, cluster, config, eventList, rand);
                case EPOCH_TIMEOUT ->
                        EpochTimeoutAction.timeout(cluster);
                case COMMIT_COMPLETED ->
                        CommitOperationAction.commit((CommitOperationEvent) nextEvent, cluster, config, eventList, rand, metrics);
                case READY_TO_COMMIT ->
                        ReadyToCommitAction.ready((ReadyToCommitEvent) nextEvent, cluster, config, eventList, rand);
            }
        }
    }
}

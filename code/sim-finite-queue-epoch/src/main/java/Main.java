import action.AbortOperationAction;
import action.ArrivalAction;
import action.ReadyToCommitAction;
import action.RepairAction;
import event.AbortOperationEvent;
import event.ArrivalEvent;
import event.ReadyToCommitEvent;
import event.RepairEvent;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import action.CommitOperationAction;
import action.EpochTimeoutAction;
import action.FailureAction;
import action.TransactionAction;
import event.AbstractEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.FailureEvent;
import event.TransactionEvent;
import event.CommitOperationEvent;
import state.ArrivalQueue;
import state.Cluster;
import utils.Clock;

import org.apache.log4j.Logger;
import utils.Config;
import utils.EventList;
import utils.Metrics;
import utils.Rand;
import utils.WriteOutArrivals;
import utils.WriteOutPerEpochRespTime;
import utils.WriteOutQueue;
import utils.WriteOutResults;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

public class Main implements Callable<Integer>
{
    private final static Logger LOGGER = Logger.getLogger( Main.class.getName() );

    @Option( names = {"-n", "--clusterSize"}, description = "Cluster size" )
    private int cluster = 50;

    @Option( names = {"-a", "--epochTimeout"}, description = "Epoch timeout (ms)" )
    private int a = 500;

    @Option( names = {"-fe", "--fixedEpoch"}, description = "Fixed or random epoch timeout" )
    private String fixedEpochTimeout = "true";

    @Option( names = {"-b", "--commit"}, description = "Average commit operation rate (ms)" )
    private double commitOperationRate = 10;

    @Option( names = {"-c", "--abort"}, description = "Average abort operation rate (ms)" )
    private double abortOperationRate = 1.7;

    @Option( names = {"-mu", "--transaction"}, description = "Average transaction service rate (ms)" )
    private double transactionServiceRate = 1;

    @Option( names = {"-xi", "--failure"}, description = "Average failure rate (ms)" )
    private double xi = 20000;

    @Option( names = {"-eta", "--repair"}, description = "Average repair rate (ms)" )
    private double eta = 1000;

    @Option( names = {"-m", "--mpt"}, description = "Proportion of distributed transactions" )
    private int distTxn = 100;

    @Option( names = {"-lam", "--arrival"}, description = "Average arrival rate (ms)" )
    private double lambda = 50;

    @Option( names = {"-p", "--protocol"}, description = "Single or multi-commit protocol" )
    private String algorithm = "multi";

    @Option( names = {"-af", "--affinity"}, description = "Affinity" )
    private String affinity = "false";

    @Option( names = {"-s", "--seed"}, description = "Fix seed" )
    private String fixSeed = "false";

    @Option( names = {"-d", "--duration"}, description = "Simulation duration (secs)" )
    private double timeLimit = 3600;

    @Option( names = {"-sv", "--seed value"}, description = "Fix seed value" )
    private long seedValue = 0;

    @Override
    public Integer call()
    {

        // config
        var config = Config.getInstance();
        config.setClusterSize( cluster );
        config.setEpochTimeout( a );
        config.setCommitOperationRate( commitOperationRate );
        config.setAbortOperationRate( abortOperationRate );
        config.setTransactionServiceRate( transactionServiceRate );
        config.setFailureRate( xi );
        config.setRepairRate( eta );
        config.setFixSeed( Boolean.parseBoolean( fixSeed ) );
        config.setSeedValue( seedValue );
        config.setAlgorithm( algorithm );
        config.setPropDistributedTransactions( (double) distTxn / 100.0 );
        config.setAffinity( Boolean.parseBoolean( affinity ) );
        config.setArrivalRate( 1 / lambda );
        config.setFixedEpochTimeout( Boolean.parseBoolean( fixedEpochTimeout ) );

        // global variables
        var rand = Rand.getInstance();
        var eventList = EventList.getInstance();
        var metrics = Metrics.getInstance();
        var queue = ArrivalQueue.getInstance();

        // coordinator node initialisation event
        var initEventTime = rand.generateNextEpochTimeout();
        var epochEvent = new EpochTimeoutEvent( initEventTime, EventType.EPOCH_TIMEOUT, 0 );
        eventList.addEvent( epochEvent );

        // initial arrival event
        var initArrivalTime = rand.generateNextArrivalTime();
        var arrivalEvent = new ArrivalEvent( initArrivalTime, EventType.TRANSACTION_ARRIVAL );
        eventList.addEvent( arrivalEvent );

        // initial failure event
        var failedNodeId = rand.generateNodeId();
        var firstFailureTime = rand.generateNextFailure();
        firstFailureTime = 10;
        var failureEvent = new FailureEvent( firstFailureTime, EventType.FAILURE, failedNodeId );
        eventList.addEvent( failureEvent );

        // run simulation
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern( "yyyy/MM/dd HH:mm:ss" );
        LocalDateTime now = LocalDateTime.now();

        LOGGER.info( "--------------------" );
        LOGGER.info( String.format( "Starting simulation at %s", dtf.format( now ) ) );
        LOGGER.info( "Simulation time (secs): " + String.format( "%.5f", timeLimit ) );
        LOGGER.info( "Configuration: " + Config.getInstance() );
        var start = System.currentTimeMillis();
        LOGGER.info( "Simulating...." );
        runSimulation( timeLimit, config, rand, eventList, queue );
        var end = System.currentTimeMillis();
        metrics.getSummary();
        var realTime = (end - start) / 1000.0;
        LOGGER.info( "Real time (secs): " + String.format( "%.5f", realTime ) );
        LOGGER.info( "Simulation completed!" );
        LOGGER.info( "--------------------" );
        LOGGER.info( "" );

        WriteOutResults.writeOutResults( config, metrics, realTime, timeLimit );
        WriteOutQueue.writeOutQueue( config, metrics );
        WriteOutPerEpochRespTime.writeOutPerEpochRespTime( config, metrics );
        WriteOutArrivals.writeOutArrivals( config, metrics );

        assert (metrics.getFailureEvents() == (metrics.getTotallyFailedEpochs() + metrics.getPartialFailedEpochs()));

        return 0;
    }

    public static void main( String[] args )
    {
        int exitCode = new CommandLine( new Main() ).execute( args );
        System.exit( exitCode );
    }

    static void runSimulation( double timeLimit, Config config, Rand rand, EventList eventList, ArrivalQueue queue )
    {
        var cluster = Cluster.getInstance();
        var clock = Clock.getInstance();
        var metrics = Metrics.getInstance();

        while ( clock.getClock() < timeLimit )
        {
            AbstractEvent nextEvent = eventList.getNextEvent();
            clock.setClock( nextEvent.getEventTime() );

            var eventType = nextEvent.getEventType();

            switch ( eventType )
            {
            case TRANSACTION_ARRIVAL -> ArrivalAction.arrival( (ArrivalEvent) nextEvent, queue, rand, eventList, cluster );
            case TRANSACTION_COMPLETED -> TransactionAction.execute( (TransactionEvent) nextEvent, cluster, config, eventList, rand, queue );
            case EPOCH_TIMEOUT -> EpochTimeoutAction.timeout( (EpochTimeoutEvent) nextEvent, cluster, config, eventList, rand, queue );
            case COMMIT_COMPLETED -> CommitOperationAction.commit( (CommitOperationEvent) nextEvent,
                    cluster, config, eventList, rand, metrics, queue );
            case ABORT_COMPLETED -> AbortOperationAction.abort( (AbortOperationEvent) nextEvent,
                    cluster, config, eventList, rand, metrics, queue );
            case FAILURE -> FailureAction.fail( (FailureEvent) nextEvent, config, cluster, eventList, rand, metrics, queue );
            case REPAIR -> RepairAction.repair( (RepairEvent) nextEvent, cluster, metrics, rand, eventList, queue );
            case READY_TO_COMMIT -> ReadyToCommitAction.ready( (ReadyToCommitEvent) nextEvent, cluster, config, eventList, rand );
            }
        }
    }
}

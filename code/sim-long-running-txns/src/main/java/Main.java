import action.AbortOperationAction;
import action.ReadyToCommitAction;
import event.AbortOperationEvent;
import event.ReadyToCommitEvent;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import action.CommitOperationAction;
import action.EpochTimeoutAction;
import action.TransactionAction;
import event.AbstractEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.TransactionEvent;
import event.CommitOperationEvent;
import state.Cluster;
import utils.Clock;

import org.apache.log4j.Logger;
import utils.Config;
import utils.EventList;
import utils.FailureRepairEventList;
import utils.Metrics;
import utils.Rand;
import utils.WriteOutResults;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Callable;

public class Main implements Callable<Integer>
{
    private final static Logger LOGGER = Logger.getLogger( Main.class.getName() );

    @Option( names = {"-n", "--cluster"}, description = "Cluster size" )
    private int cluster = 64;

    @Option( names = {"-a", "--epoch"}, description = "Epoch timeout (ms)" )
    private int epochTimeout = 100;

    @Option( names = {"-b", "--commit"}, description = "Average commit operation rate (ms)" )
    private double commitOperationRate = 1.7;

    @Option( names = {"-c", "--abort"}, description = "Average abort operation rate (ms)" )
    private double abortOperationRate = 1.7;

    @Option( names = {"-mu", "--transaction"}, description = "Average transaction service rate (ms)" )
    private double transactionServiceRate = 1;

    @Option( names = {"-s", "--seed"}, description = "Fix seed" )
    private String fixSeed = "false";

    @Option( names = {"-d", "--duration"}, description = "Simulation duration (secs)" )
    private double timeLimit = 3600;

    @Option( names = {"-p", "--protocol"}, description = "Single or multi-commit protocol" )
    private String algorithm = "single";

    @Option( names = {"-m", "--mpt"}, description = "Proportion of distributed transactions" )
    private int distTxn = 10;

    @Option( names = {"-af", "--affinity"}, description = "Affinity" )
    private String affinity = "false";

    @Option( names = {"-fe", "--fixedEpoch"}, description = "Fixed or random epoch timeout" )
    private String fixedEpochTimeout = "true";

    @Override
    public Integer call()
    {
        // config
        var config = Config.getInstance();
        config.setClusterSize( cluster );
        config.setEpochTimeout( epochTimeout );
        config.setCommitOperationRate( commitOperationRate );
        config.setAbortOperationRate( abortOperationRate );
        config.setTransactionServiceRate( transactionServiceRate );
        config.setFixSeed( Boolean.parseBoolean( fixSeed ) );
        config.setAlgorithm( algorithm );
        config.setPropDistributedTransactions( (double) distTxn / 100.0 );
        config.setAffinity( Boolean.parseBoolean( affinity ) );
        config.setFixedEpochTimeout( Boolean.parseBoolean( fixedEpochTimeout ) );

        // global variables
        var rand = Rand.getInstance();
        var eventList = EventList.getInstance();
        var metrics = Metrics.getInstance();
        var cluster = Cluster.getInstance();
        var failureRepairEventList = FailureRepairEventList.getInstance();

        // coordinator node initialisation event
        var initEventTime = rand.generateNextEpochTimeout();
        var epochEvent = new EpochTimeoutEvent( initEventTime, EventType.EPOCH_TIMEOUT, 0 );
        eventList.addEvent( epochEvent );

        // initial transaction events
        var clusterSize = config.getClusterSize();
        for ( int i = 0; i < clusterSize; i++ )
        {
            var transactionEvent = new TransactionEvent( rand.generateTransactionServiceTime(), EventType.TRANSACTION_COMPLETED, i, 0 );
            eventList.addEvent( transactionEvent );
        }

        // run simulation
        DateTimeFormatter dtf = DateTimeFormatter.ofPattern( "yyyy/MM/dd HH:mm:ss" );
        LocalDateTime now = LocalDateTime.now();

        LOGGER.info( "--------------------" );
        LOGGER.info( String.format( "Starting simulation at %s", dtf.format( now ) ) );
        LOGGER.info( "Simulation time (secs): " + String.format( "%.5f", timeLimit ) );
        LOGGER.info( "Configuration: " + Config.getInstance() );
        var start = System.currentTimeMillis();
        Clock.getInstance().setSimStartTime( start );
        LOGGER.info( "Simulating...." );
        runSimulation( timeLimit, config, rand, eventList, failureRepairEventList );
        var end = System.currentTimeMillis();
        metrics.getSummary( config, cluster );
        var realTime = (end - start) / 1000.0;
        LOGGER.info( "Real time (secs): " + String.format( "%.5f", realTime ) );
        LOGGER.info( "Simulation completed!" );
        LOGGER.info( "--------------------" );
        LOGGER.info( "" );

        WriteOutResults.writeOutResults( config, metrics, realTime, timeLimit );

        assert (metrics.getFailureEvents() == (metrics.getTotallyFailedEpochs() + metrics.getPartiallyFailedEpochs()));

        return 0;
    }

    public static void main( String[] args )
    {
        int exitCode = new CommandLine( new Main() ).execute( args );
        System.exit( exitCode );
    }

    static void runSimulation( double timeLimit, Config config, Rand rand, EventList eventList, FailureRepairEventList failureRepairEventList )
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
            case TRANSACTION_COMPLETED -> TransactionAction.execute( (TransactionEvent) nextEvent, cluster, config, eventList, rand );
            case EPOCH_TIMEOUT -> EpochTimeoutAction.timeout( (EpochTimeoutEvent) nextEvent, cluster, config, eventList, rand );
            case COMMIT_COMPLETED -> CommitOperationAction.commit( (CommitOperationEvent) nextEvent,
                    cluster, config, eventList, rand, metrics, failureRepairEventList );
            case ABORT_COMPLETED -> AbortOperationAction.abort( (AbortOperationEvent) nextEvent,
                    cluster, config, eventList, rand, metrics, failureRepairEventList );
            case READY_TO_COMMIT -> ReadyToCommitAction.ready( (ReadyToCommitEvent) nextEvent, cluster, config, eventList, rand );
            }
        }
    }
}

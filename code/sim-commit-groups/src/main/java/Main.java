import action.ReadyToCommitAction;
import event.ReadyToCommitEvent;
import picocli.CommandLine;
import picocli.CommandLine.Option;

import action.EpochTimeoutAction;
import action.DistributedTransactionAction;
import event.AbstractEvent;
import event.EpochTimeoutEvent;
import event.EventType;
import event.DistributedTransactionEvent;
import state.Cluster;
import utils.Clock;

import org.apache.log4j.Logger;
import utils.Config;
import utils.EventList;
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
    private int epochTimeout = 10;

    @Option( names = {"-thpt", "--throughput"}, description = "System throughput (completed transactions/s)" )
    private int txnPerSec = 250000;

    @Option( names = {"-m", "--mpt"}, description = "Proportion of distributed transactions" )
    private double propDist = 0.125;

    @Option( names = {"-s", "--seed"}, description = "Fix seed" )
    private String fixSeed = "false";

    @Option( names = {"-sv", "--seedValue"}, description = "Seed value" )
    private int seedValue = 0;

    @Option( names = {"-d", "--duration"}, description = "Simulation duration (secs)" )
    private double timeLimit = 10;

    @Override
    public Integer call()
    {
        var config = Config.getInstance();
        var metrics = Metrics.getInstance();
        config.setClusterSize( cluster );
        config.setEpochTimeout( epochTimeout );

        if ( propDist != 0 )
        {
            var epochsPerSec = 1000 / epochTimeout;
            var txnPerEpoch = txnPerSec / epochsPerSec;
            var txnPerNodePerEpoch = txnPerEpoch / cluster;
            var distTxnPerNodePerEpoch = txnPerNodePerEpoch * propDist;
            var rate = epochTimeout / distTxnPerNodePerEpoch;

            // config
            config.setTransactionServiceRate( rate );
            config.setFixSeed( Boolean.parseBoolean( fixSeed ) );
            config.setSeedValue( seedValue );

            // global variables
            var rand = Rand.getInstance();
            var eventList = EventList.getInstance();

            // coordinator node initialisation event
            var initEventTime = rand.generateNextEpoch();
            var epochEvent = new EpochTimeoutEvent( initEventTime, EventType.EPOCH_TIMEOUT );
            eventList.addEvent( epochEvent );

            // initial transaction events
            var clusterSize = config.getClusterSize();
            for ( int i = 0; i < clusterSize; i++ )
            {
                var transactionEvent = new DistributedTransactionEvent( rand.generateTransactionServiceTime(), EventType.DIST_TXN_COMPLETED, i );
                eventList.addEvent( transactionEvent );
            }

            // run simulation
            DateTimeFormatter dtf = DateTimeFormatter.ofPattern( "yyyy/MM/dd HH:mm:ss" );
            LocalDateTime now = LocalDateTime.now();

            LOGGER.info( "--------------------" );
            LOGGER.info( String.format( "Starting simulation at %s", dtf.format( now ) ) );
            LOGGER.info( "Simulation time (secs): " + String.format( "%.5f", timeLimit ) );

            LOGGER.info( "Transactions/s: " + txnPerSec );
            LOGGER.info( "Epochs/s: " + txnPerSec );
            LOGGER.info( "Transactions/epoch: " + txnPerEpoch );
            LOGGER.info( "Transactions/epoch/node: " + txnPerNodePerEpoch );
            LOGGER.info( "Distributed transactions/epoch/node: " + distTxnPerNodePerEpoch );
            LOGGER.info( "Distributed transaction service rate (ms): " + rate );
            LOGGER.info( "Proportion of distributed transactions: " + propDist );

            LOGGER.info( "Configuration: " + Config.getInstance() );
            var start = System.currentTimeMillis();
            LOGGER.info( "Simulating...." );
            runSimulation( timeLimit, config, rand, eventList );
            var end = System.currentTimeMillis();
            metrics.getSummary();
            var realTime = (end - start) / 1000.0;
            LOGGER.info( "Real time (secs): " + String.format( "%.5f", realTime ) );
            LOGGER.info( "Simulation completed!" );
            LOGGER.info( "--------------------" );
            LOGGER.info( "" );
        }
        else
        {
            config.setTransactionServiceRate( 0 );
            metrics.incCompletedEpochs(); // set to 1
            metrics.incNumberOfCommitGroups( config.getClusterSize() );
        }

        WriteOutResults.writeOutResults( config, metrics, propDist );

        return 0;
    }

    public static void main( String[] args )
    {
        int exitCode = new CommandLine( new Main() ).execute( args );
        System.exit( exitCode );
    }

    static void runSimulation( double timeLimit, Config config, Rand rand, EventList eventList )
    {
        var cluster = Cluster.getInstance();
        var clock = Clock.getInstance();
        var metrics = Metrics.getInstance();

        while ( clock.getClock() < timeLimit )
        {

            AbstractEvent nextEvent = eventList.getNextEvent();
            clock.setClock( nextEvent.getEventTime() );

            LOGGER.debug( "Event: " + nextEvent.getEventType() + " at " + String.format( "%.2f (ms)", nextEvent.getEventTime() * 1000.0 ) );
            LOGGER.debug( "Action(s): " );

            var eventType = nextEvent.getEventType();
            switch ( eventType )
            {
            case DIST_TXN_COMPLETED -> DistributedTransactionAction.execute( (DistributedTransactionEvent) nextEvent, cluster, config, eventList, rand );
            case EPOCH_TIMEOUT -> EpochTimeoutAction.timeout( cluster );
            case READY_TO_COMMIT -> ReadyToCommitAction.ready( (ReadyToCommitEvent) nextEvent, cluster, config, eventList, rand );
            }
            LOGGER.debug( "" );
        }
    }
}

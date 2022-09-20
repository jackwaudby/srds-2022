package state;

import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.BiconnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import utils.Config;
import utils.Metrics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

public class Cluster
{
    public static class Failure
    {
        int nodeId;
        double eventTime;
        EpochState clusterState;

        public Failure( int nodeId, double eventTime, EpochState clusterState )
        {
            this.nodeId = nodeId;
            this.eventTime = eventTime;
            this.clusterState = clusterState;
        }

        public double getEventTime()
        {
            return eventTime;
        }

        @Override
        public String toString()
        {
            return "{" +
                   "id=" + nodeId +
                   ",t=" + String.format( "%.3f", eventTime ) +
                   ",s=" + clusterState +
                   '}';
        }
    }

    private final static Logger LOGGER = Logger.getLogger( Cluster.class.getName() );

    private static final Cluster instance = new Cluster();

    private double start;
    private Integer currentEpoch;
    private EpochState currentEpochState;
    private final List<Integer> completedJobs;
    private final List<Integer> lostJobs;
    private final List<NodeState> nodeStates;
    private final List<EpochSummary> history;
    private Set<Failure> nodeFailureSet;
    private final Set<Integer> nodeRepairSet;
    private Graph<Integer,DefaultEdge> dependencyGraph;
    private Map<Integer,CommitGroup> commitGroupMap;
    private boolean clusterDown;
    private final List<Job> currentJob;
    private final List<List<Job>> completedJobStack;

    private Cluster()
    {
        this.start = 0.0;
        this.currentEpoch = 0;
        this.currentEpochState = EpochState.PROCESSING;
        var clusterSize = Config.getInstance().getClusterSize();
        this.completedJobs = new ArrayList<>();
        this.lostJobs = new ArrayList<>();
        this.nodeStates = new ArrayList<>();
        this.nodeFailureSet = new HashSet<>();
        this.nodeRepairSet = new HashSet<>();
        dependencyGraph = new SimpleGraph<>( DefaultEdge.class );
        this.commitGroupMap = new HashMap<>();
        this.clusterDown = false;
        this.currentJob = new ArrayList<>();
        this.completedJobStack = new ArrayList<>();

        for ( int i = 0; i < clusterSize; i++ )
        {
            dependencyGraph.addVertex( i );
            completedJobs.add( 0 );
            lostJobs.add( 0 );
            nodeStates.add( NodeState.IDLE );
            currentJob.add( null );
            completedJobStack.add( new ArrayList<>() );
        }

        this.history = new ArrayList<>();
    }

    public static Cluster getInstance()
    {
        return instance;
    }

    public void setCurrentJob( int nodeId, Job job )
    {
        currentJob.set( nodeId, job );
    }

    public Job getCurrentJob( int nodeId )
    {
        return currentJob.get( nodeId );
    }

    public void addToCompletedJobStack( Job job, int nodeId )
    {
        completedJobStack.get( nodeId ).add( job );
    }

    public boolean hasInFlightJob( int id )
    {
        return currentJob.get( id ) != null && getNodeState( id ) == NodeState.OPERATIONAL;
    }

    public int getIdleNodeId()
    {
        for ( int i = 0; i < currentJob.size(); i++ )
        {
            if ( getNodeState( i ) == NodeState.IDLE )
            {
                return i;
            }
        }

        return -1;
    }

    public boolean isClusterDown()
    {
        return clusterDown;
    }

    public boolean epochContainsFailureEvent()
    {
        return !nodeFailureSet.isEmpty();
    }

    public boolean isNodeFailureSetEmpty()
    {
        return nodeFailureSet.isEmpty();
    }

    public void recordFailureEvent( int failedNodeId, double eventTime, EpochState currentEpochState )
    {
        this.nodeFailureSet.add( new Failure( failedNodeId, eventTime, currentEpochState ) );
    }

    public void recordRepairEvent( int repairedNodeId )
    {
        this.nodeRepairSet.add( repairedNodeId );
    }

    public void addDependency( int nodeA, int nodeB )
    {
        if ( !this.dependencyGraph.containsEdge( nodeA, nodeB ) )
        {
            this.dependencyGraph.addEdge( nodeA, nodeB );
        }
    }

    public EpochState getCurrentEpochState()
    {
        return currentEpochState;
    }

    public Integer getCurrentEpoch()
    {
        return currentEpoch;
    }

    public boolean areAllOperationalNodesReadyToCommit( Config config )
    {
        for ( int i = 0; i < config.getClusterSize(); i++ )
        {
            if ( getNodeState( i ) == NodeState.OPERATIONAL )
            {
                return false; // one node is not ready yet
            }
        }

        return true;
    }

    public List<Job> getCompletedJobStack( int nodeId )
    {
        return completedJobStack.get( nodeId );
    }

    public void complete( ArrivalQueue queue, Config config, Metrics metrics, double end, boolean partial )
    {
        // record the number of operational commit groups
        if ( Objects.equals( "multi", config.getAlgorithm() ) && !isNodeFailureSetEmpty() )
        {
            metrics.incOperationalCommitGroups( getNumberOfOperationalCommitGroups() );
        }

        if ( partial )
        {
            metrics.incPartialFailures();

            // move completed jobs in failed commit groups to lost jobs
            for ( int nodeId = 0; nodeId < config.getClusterSize(); nodeId++ )
            {
                var nodeState = getNodeState( nodeId );
                if ( nodeState == NodeState.CRASHED || nodeState == NodeState.IN_FAILED_COMMIT_GROUP )
                {
                    LOGGER.debug( String.format( " - node %s has state %s", nodeId, nodeState ) );

                    // move counters
                    moveCompletedToLostJobs( nodeId, 0 );
                    // move jobs
                    var jobsToRetry = getCompletedJobStack( nodeId );
                    for ( var job : jobsToRetry )
                    {
                        job.incRetries();
                    }
                    queue.addJobs( jobsToRetry );
                }
            }
        }
        else
        {
            metrics.incCompletedEpochs();
        }

        double perEpochCumRespTime = 0.0;

        // for each completed job compute response time
        for ( List<Job> completedJobs : completedJobStack )
        {
            for ( Job completedJob : completedJobs )
            {
                completedJob.setDepartureTime( end );
                metrics.incCumulativeResponseTime( completedJob.responseTime() );
                perEpochCumRespTime += completedJob.responseTime();
            }
        }

        // record cumulative latency
        var duration = end - this.start;
        metrics.incCumulativeLatency( duration );
        this.start = end;

        // compute job totals
        var totalLostJobs = lostJobs.stream().mapToInt( a -> a ).sum();
        metrics.incLostJobs( totalLostJobs );
//        metrics.incFailedEpochLostJobs( totalLostJobs ); // multi only
        var totalCompletedJobs = completedJobs.stream().mapToInt( a -> a ).sum();
        metrics.incCompletedTransactions( totalCompletedJobs );

        metrics.addPerEpochAvRespTime( perEpochCumRespTime / totalCompletedJobs );
    }

    public void totalFailure( Config config, Metrics metrics, double endEventTime, ArrivalQueue queue )
    {
        // increment failed epochs
        metrics.incTotallyFailedEpochs();

        // record the number of operational commit groups (this will always be 0)
        if ( Objects.equals( "multi", config.getAlgorithm() ) )
        {
            metrics.incOperationalCommitGroups( getNumberOfOperationalCommitGroups() );
        }

        // record cumulative latency of the simulation
        var duration = endEventTime - this.start;
        metrics.incCumulativeLatency( duration );
        this.start = endEventTime;

        // move completed job to arrival queue
        for ( int nodeId = 0; nodeId < config.getClusterSize(); nodeId++ )
        {
            var jobsToRetry = getCompletedJobStack( nodeId );
            for ( var job : jobsToRetry )
            {
                job.incRetries();
            }
            queue.addJobs( jobsToRetry );
        }

        // compute job totals
        var totalLostJobs = lostJobs.stream().mapToInt( a -> a ).sum();
        var totalCompletedJobs = completedJobs.stream().mapToInt( a -> a ).sum();
        totalLostJobs = totalLostJobs + totalCompletedJobs;
        metrics.incFailedEpochLostJobs( totalLostJobs );
        metrics.incLostJobs( totalLostJobs );

        System.out.println( "Lost jobs: " + totalLostJobs );

//        metrics.addPerEpochAvRespTime( 0 );
    }

    public long getNumberOfOperationalNodes()
    {
        return nodeStates.stream().filter( state -> state != NodeState.CRASHED ).count();
    }

    public void resetClusterState( Config config, Metrics metrics )
    {
        var clusterSize = config.getClusterSize();

        for ( int nodeId = 0; nodeId < clusterSize; nodeId++ )
        {
            // if node was ready or in a failed commit group then switch back to idle
            // CRASHED nodes stay crashed
            if ( getNodeState( nodeId ) == NodeState.READY || getNodeState( nodeId ) == NodeState.IN_FAILED_COMMIT_GROUP )
            {
                setNodeState( nodeId, NodeState.IDLE );
            }

            completedJobs.set( nodeId, 0 ); // reset completed work
            lostJobs.set( nodeId, 0 ); // reset aborted work
            completedJobStack.get( nodeId ).clear(); // reset completed job stack
        }

        currentEpoch += 1; // increment epoch
//        System.out.printf( "Sim clock: %.5f, Current epoch: %s, Failure events: %s, Operational nodes: %s\r", Clock.getInstance().getClock(), currentEpoch,
//                metrics.getFailureEvents(), getNumberOfOperationalNodes() );
        currentEpochState = EpochState.PROCESSING; // set to processing

        if ( Objects.equals( config.getAlgorithm(), "multi" ) )
        {
            Graph<Integer,DefaultEdge> clean = new SimpleGraph<>( DefaultEdge.class );

            for ( int i = 0; i < clusterSize; i++ )
            {
                clean.addVertex( i );
            }
            this.dependencyGraph = clean;
            this.commitGroupMap = new HashMap<>();
            this.nodeFailureSet = new HashSet<>();
        }

        this.nodeFailureSet.clear(); // clear trackers
        this.nodeRepairSet.clear();

        metrics.recordPerEpochArrivals();
    }

    public void setCurrentEpochState( EpochState currentEpochState )
    {
        this.currentEpochState = currentEpochState;
    }

    public List<EpochSummary> getHistory()
    {
        return history;
    }

    public void incCompletedJobs( int nodeId )
    {
        var curr = this.completedJobs.get( nodeId ) + 1;
        this.completedJobs.set( nodeId, curr );
    }

    public void incLostJobs( int nodeId )
    {
        var curr = this.lostJobs.get( nodeId ) + 1;
        this.lostJobs.set( nodeId, curr );
    }

    public void setNodeState( int nodeId, NodeState state )
    {
        this.nodeStates.set( nodeId, state );

        // check is system is down
        for ( var nodeState : nodeStates )
        {
            if ( nodeState != NodeState.CRASHED )
            {
                // at least 1 node is up
                return;
            }
        }

        clusterDown = true;
    }

    public NodeState getNodeState( int nodeId )
    {
        return nodeStates.get( nodeId );
    }

    public void computeCommitGroups()
    {
        Map<Integer,CommitGroup> commitGroups = new HashMap<>();
        var scAlgo = new BiconnectivityInspector<>( dependencyGraph );
        var connectedComponents = scAlgo.getConnectedComponents();
        int commitGroupId = 0;

        for ( var component : connectedComponents )
        {
            var members = component.vertexSet();
            var commitGroup = new CommitGroup( commitGroupId, members );
            commitGroups.put( commitGroupId, commitGroup );
            commitGroupId += 1;
        }
        this.commitGroupMap = commitGroups;
    }

    public void transitionNodesInCrashedGroupsToFailed()
    {
        for ( var commitGroup : commitGroupMap.values() )
        {
            if ( containsCrashedNode( commitGroup ) )
            {
                var members = commitGroup.getMembers();

                for ( int i = 0; i < members.size(); i++ )
                {
                    var memberId = members.get( 0 );
                    var memberState = getNodeState( memberId );
                    if ( memberState == NodeState.READY )
                    {
                        setNodeState( memberId, NodeState.IN_FAILED_COMMIT_GROUP );
                    }
                }
            }
        }
    }

    public int getNumberOfOperationalCommitGroups()
    {
        int operationalCommitGroups = commitGroupMap.size();
        for ( var commitGroup : commitGroupMap.values() )
        {
            // contains a crashed node
            if ( containsCrashedNode( commitGroup ) )
            {
                operationalCommitGroups -= 1;
            }
        }

        return operationalCommitGroups;
    }

    public int getNumberOfCrashedCommitGroups()
    {
        int crashedCommitGroups = 0;
        for ( var commitGroup : commitGroupMap.values() )
        {
            // contains a crashed node
            if ( containsCrashedNode( commitGroup ) )
            {
                crashedCommitGroups += 1;
            }
        }

        return crashedCommitGroups;
    }

    public boolean containsCrashedNode( CommitGroup commitGroup )
    {
        for ( var member : commitGroup.getMembers() )
        {
            var state = getNodeState( member );
            if ( state == NodeState.CRASHED )
            {
                return true;
            }
        }
        return false;
    }

    public void moveCompletedToLostJobs( Integer member, int extra )
    {
        var curr = lostJobs.get( member );
        int toAdd = completedJobs.get( member ) + extra;
        lostJobs.set( member, curr + toAdd );
        completedJobs.set( member, 0 );
        LOGGER.debug( String.format( " - move %s completed jobs to lost", toAdd ) );
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "start=" + start +
               ", epoch=" + currentEpoch +
               ", state=" + currentEpochState +
               ", completedJobs=" + completedJobs +
               ", lostJobs=" + lostJobs +
               ", nodeStates=" + nodeStates +
               ", failures=" + nodeFailureSet +
               ", dependencyGraph=" + dependencyGraph +
               ", commitGroupMap=" + commitGroupMap +
               '}';
    }
}

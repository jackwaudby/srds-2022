package state;

import org.jgrapht.Graph;
import org.jgrapht.alg.connectivity.BiconnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleGraph;
import utils.Clock;
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

        public Failure( int nodeId, double eventTime )
        {
            this.nodeId = nodeId;
            this.eventTime = eventTime;
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
                   '}';
        }
    }

    private static final Cluster instance = new Cluster();

    double start;
    Integer currentEpoch;
    EpochState currentEpochState;
    List<Integer> completedJobs;
    List<Integer> jobsLostAccessingCrashedNodes;
    List<Integer> jobsLostInFlight;
    List<Integer> lostCompletedJobs;

    List<NodeState> nodeStates;
    Set<Failure> nodeFailureSet;
    Set<Integer> nodeRepairSet;
    boolean skipped;
    Graph<Integer,DefaultEdge> dependencyGraph;
    Map<Integer,CommitGroup> commitGroupMap;
    boolean clusterDown;
    int numCommitGroupsAtFailure;

    private Cluster()
    {
        this.start = 0.0;
        this.skipped = false;
        this.currentEpoch = 0;
        this.currentEpochState = EpochState.PROCESSING;
        var clusterSize = Config.getInstance().getClusterSize();
        this.completedJobs = new ArrayList<>();

        this.jobsLostAccessingCrashedNodes = new ArrayList<>();
        this.jobsLostInFlight = new ArrayList<>();
        this.lostCompletedJobs = new ArrayList<>();

        this.nodeStates = new ArrayList<>();
        this.nodeFailureSet = new HashSet<>();
        this.nodeRepairSet = new HashSet<>();
        dependencyGraph = new SimpleGraph<>( DefaultEdge.class );
        this.commitGroupMap = new HashMap<>();
        this.clusterDown = false;
        this.numCommitGroupsAtFailure = 0;

        for ( int i = 0; i < clusterSize; i++ )
        {
            dependencyGraph.addVertex( i );
            completedJobs.add( 0 );
            jobsLostAccessingCrashedNodes.add( 0 );
            jobsLostInFlight.add( 0 );
            lostCompletedJobs.add( 0 );
            nodeStates.add( NodeState.OPERATIONAL );
        }
    }

    public static Cluster getInstance()
    {
        return instance;
    }

    public boolean isSkipped()
    {
        return skipped;
    }

    public void setSkipped( boolean skipped )
    {
        this.skipped = skipped;
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

    public void recordFailureEvent( int failedNodeId, double eventTime )
    {
        this.nodeFailureSet.add( new Failure( failedNodeId, eventTime ) );
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
            var nodeState = getNodeState( i );
            if ( nodeState == NodeState.OPERATIONAL )
            {
                return false; // one node is not ready yet
            }
        }

        return true;
    }

    private boolean isMultipleFailures()
    {
        return this.nodeFailureSet.size() > 1;
    }

    public void complete( Metrics metrics, double end, boolean partial, Config config )
    {
        // if there has been a failure in this epoch then record the number of operational commit groups
        if ( Objects.equals( "multi", config.getAlgorithm() ) && !isNodeFailureSetEmpty() )
        {
            metrics.incOperationalCommitGroups( getNumberOfOperationalCommitGroups() );
        }

        if ( partial )
        {
            metrics.incPartiallyFailedEpochs();
            metrics.incCyclesWithFailures();

            if ( isMultipleFailures() )
            {
                metrics.incCyclesWithMultipleFailures();
            }

            // move completed jobs in failed commit groups to
            for ( int i = 0; i < config.getClusterSize(); i++ )
            {
                var nodeState = getNodeState( i );
                if ( nodeState == NodeState.CRASHED || nodeState == NodeState.IN_FAILED_COMMIT_GROUP )
                {
                    moveCompletedToLostJobs( i );
                }
            }

            var totalCompletedJobs = completedJobs.stream().mapToInt( a -> a ).sum();
            metrics.incCommittedTransactionsDuringFailures( totalCompletedJobs );
        }
        else
        {
            metrics.incCompletedEpochs();
        }

        // record cumulative latency
        var duration = end - this.start;
        metrics.incCumulativeLatency( duration );
        this.start = end;

        // compute job totals
        var totalLostJobsAccessingCrashedNodes = jobsLostAccessingCrashedNodes.stream().mapToInt( a -> a ).sum();
        var totalLostJobsInFlight = jobsLostInFlight.stream().mapToInt( a -> a ).sum();
        var totalCompletedJobsLost = lostCompletedJobs.stream().mapToInt( a -> a ).sum();

        metrics.incLostAccessingCrashedNode( totalLostJobsAccessingCrashedNodes );
        metrics.incLostInFlightJobs( totalLostJobsInFlight );
        metrics.incLostCompletedJobs( totalCompletedJobsLost );

        var totalCompletedJobs = completedJobs.stream().mapToInt( a -> a ).sum();
        metrics.incCompletedTransactions( totalCompletedJobs );

        var totalLost = totalLostJobsInFlight + totalCompletedJobsLost + totalLostJobsAccessingCrashedNodes;
        var totalJob = String.format( "Epoch: %s, completed: %s, lost: %s, dur: %.4f", currentEpoch, totalCompletedJobs, totalLost, duration );
//        System.out.println( totalJob );
    }

    public void totalFailure( Metrics metrics, double endEventTime )
    {
        // if there has been a failure in this epoch then record the number of operational commit groups
        if ( Objects.equals( "multi", Config.getInstance().getAlgorithm() ) )
        {
            metrics.incOperationalCommitGroups( getNumberOfOperationalCommitGroups() );
        }

        // increment failed epochs
        metrics.incTotallyFailedEpochs();
        metrics.incCyclesWithFailures();

        if ( isMultipleFailures() )
        {
            metrics.incCyclesWithMultipleFailures();
        }

        // record cumulative latency
        var duration = endEventTime - this.start;
        metrics.incCumulativeLatency( duration );
        this.start = endEventTime;

        // compute job totals
        var totalLostJobsAccessingCrashedNodes = jobsLostAccessingCrashedNodes.stream().mapToInt( a -> a ).sum();
        var totalLostJobsInFlight = jobsLostInFlight.stream().mapToInt( a -> a ).sum();
        var totalCompletedJobsLost = lostCompletedJobs.stream().mapToInt( a -> a ).sum();
        var totalCompletedJobs = completedJobs.stream().mapToInt( a -> a ).sum();

        metrics.incLostAccessingCrashedNode( totalLostJobsAccessingCrashedNodes );
        metrics.incLostInFlightJobs( totalLostJobsInFlight );
        metrics.incLostCompletedJobs( totalCompletedJobsLost + totalCompletedJobs );

        var totalLost = totalLostJobsInFlight + totalCompletedJobsLost + totalLostJobsAccessingCrashedNodes;
        var totalJob = String.format( "Epoch: %s, completed: %s, lost: %s, dur: %.3f", currentEpoch, totalCompletedJobs, totalLost, duration );
//        System.out.println( totalJob );
    }

    public void resetClusterState( Config config, Metrics metrics )
    {
        var clusterSize = config.getClusterSize();
        // foreach node in the cluster
        for ( int i = 0; i < clusterSize; i++ )
        {
            if ( getNodeState( i ) == NodeState.READY || getNodeState( i ) == NodeState.IN_FAILED_COMMIT_GROUP )
            {
                setNodeState( i, NodeState.OPERATIONAL );
            }

            completedJobs.set( i, 0 ); // reset completed work
            jobsLostAccessingCrashedNodes.set( i, 0 ); // reset aborted work
            jobsLostInFlight.set( i, 0 );
            lostCompletedJobs.set( i, 0 );
        }

        currentEpoch += 1; // increment epoch
//        var v = Math.floor( Clock.getInstance().getClock() );
//        if ( v % (60 * 60) == 0 )
//        {
//            var currTime = System.currentTimeMillis();
//            var currDur = (currTime - Clock.getInstance().getSimStartTime()) / 1000.0 / 60;
//            System.out.printf( "Sim clock (hr): %.1f, Real time (min): %.0f, Current epoch: %s, Failure events: %s\r", v / (60 * 60), currDur, currentEpoch,
//                    metrics.getFailureEvents() );
//        }

        currentEpochState = EpochState.PROCESSING; // set to processing
        numCommitGroupsAtFailure = 0;

        var algo = config.getAlgorithm();
        if ( Objects.equals( algo, "multi" ) )
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
    }

    public void setCurrentEpochState( EpochState currentEpochState )
    {
        this.currentEpochState = currentEpochState;
    }

    public void incCompletedJobs( int nodeId, int toAdd )
    {
        var curr = this.completedJobs.get( nodeId ) + toAdd;
        this.completedJobs.set( nodeId, curr );
    }

    public void incJobsLostAccessingCrashedNodes( int nodeId )
    {
        var curr = this.jobsLostAccessingCrashedNodes.get( nodeId ) + 1;
        this.jobsLostAccessingCrashedNodes.set( nodeId, curr );
    }

    /**
     * (i) if epoch contains a failure, multi-commit is not enabled, when epoch
     * times out, each operational node loses their in-flight jobs
     * (ii) node fails during processing, it loses its in-flight job
     * (iii) failure happens whilst cluster is waiting, each node still processing loses
     * their in-flight job
     */
    public void incInFlightJobsLost( int nodeId )
    {
        var curr = this.jobsLostInFlight.get( nodeId ) + 1;
        this.jobsLostInFlight.set( nodeId, curr );
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

//    public int getNumberOfCrashedCommitGroups()
//    {
//        int crashedCommitGroups = 0;
//        for ( var commitGroup : commitGroupMap.values() )
//        {
//            // contains a crashed node
//            if ( containsCrashedNode( commitGroup ) )
//            {
//                crashedCommitGroups += 1;
//            }
//        }
//
//        return crashedCommitGroups;
//    }

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

    public void moveCompletedToLostJobs( Integer member )
    {
        var curr = lostCompletedJobs.get( member );
        int toAdd = completedJobs.get( member );
        lostCompletedJobs.set( member, curr + toAdd );
        completedJobs.set( member, 0 );
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "start=" + start +
               ", epoch=" + currentEpoch +
               ", state=" + currentEpochState +
               ", completedJobs=" + completedJobs +
               ", nodeStates=" + nodeStates +
               ", failures=" + nodeFailureSet +
               ", dependencyGraph=" + dependencyGraph +
               ", commitGroupMap=" + commitGroupMap +
               '}';
    }
}

package state;

import action.DistributedTransactionAction;
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

public class Cluster
{

    private final static Logger LOGGER = Logger.getLogger( Cluster.class.getName() );

    private static final Cluster instance = new Cluster();

    // current epoch state
    EpochState currentEpochState;
    // node status (per node)
    List<NodeState> nodeStates;
    // dependency graph for this current epoch
    Graph<Integer,DefaultEdge> dependencyGraph;
    // commit groups for the current epoch
    Map<Integer,CommitGroup> commitGroupMap;

    private Cluster()
    {
        this.currentEpochState = EpochState.PROCESSING;
        var clusterSize = Config.getInstance().getClusterSize();
        this.nodeStates = new ArrayList<>();
        dependencyGraph = new SimpleGraph<>( DefaultEdge.class );
        this.commitGroupMap = new HashMap<>();

        for ( int i = 0; i < clusterSize; i++ )
        {
            dependencyGraph.addVertex( i );
            nodeStates.add( NodeState.OPERATIONAL );
        }
    }

    public static Cluster getInstance()
    {
        return instance;
    }

    public void addDependency( int nodeA, int nodeB )
    {
        if ( !this.dependencyGraph.containsEdge( nodeA, nodeB ) )
        {
            LOGGER.debug( String.format( " - add from %s to %s", nodeA, nodeB ) );
            this.dependencyGraph.addEdge( nodeA, nodeB );
        }
    }

    public EpochState getCurrentEpochState()
    {
        return currentEpochState;
    }

    public boolean areAllNodesReadyToCommit( Config config )
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

    public void complete( Metrics metrics )
    {
        metrics.incCompletedEpochs();
        metrics.incNumberOfCommitGroups( getNumberOfCommitGroups() );
    }

    public void resetClusterState( Config config )
    {
        var clusterSize = config.getClusterSize();
        // foreach node in the cluster
        for ( int i = 0; i < clusterSize; i++ )
        {
            setNodeState( i, NodeState.OPERATIONAL );
        }

        currentEpochState = EpochState.PROCESSING; // set to processing

        Graph<Integer,DefaultEdge> clean = new SimpleGraph<>( DefaultEdge.class );
        for ( int i = 0; i < clusterSize; i++ )
        {
            clean.addVertex( i );
        }
        this.dependencyGraph = clean;
        this.commitGroupMap = new HashMap<>();
    }

    public void setCurrentEpochState( EpochState currentEpochState )
    {
        this.currentEpochState = currentEpochState;
    }

    public void setNodeState( int nodeId, NodeState state )
    {
        this.nodeStates.set( nodeId, state );
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

    public int getNumberOfCommitGroups()
    {
        return commitGroupMap.size();
    }

    @Override
    public String toString()
    {
        return "Cluster{" +
               "state=" + currentEpochState +
               ", nodeStates=" + nodeStates +
               ", dependencyGraph=" + dependencyGraph +
               ", commitGroupMap=" + commitGroupMap +
               '}';
    }
}

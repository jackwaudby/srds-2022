package action;

import event.EpochTimeoutEvent;

import event.EventType;
import event.ReadyToCommitEvent;
import org.apache.log4j.Logger;
import state.ArrivalQueue;
import state.Cluster;
import state.EpochState;
import state.NodeState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class EpochTimeoutAction
{
    private final static Logger LOGGER = Logger.getLogger( EpochTimeoutAction.class.getName() );

    public static void timeout( EpochTimeoutEvent event, Cluster cluster, Config config, EventList eventList, Rand rand, ArrivalQueue queue )
    {

        //TODO: edge case when timeout happens whilst previous epoch is still aborting/committing

        var currentEpoch = event.getEpoch();
        var thisEventTime = event.getEventTime();

        // failures are detected at epoch timeout events
        if ( cluster.epochContainsFailureEvent() )
        {
            // multi: transition to wait and form commit groups
            if ( Objects.equals( config.getAlgorithm(), "multi" ) )
            {
                transitionToWaiting( cluster, config );
            }
            // single: immediately abort this epoch
            else
            {
                // for each operational node, with a job, retry
                for ( int nodeId = 0; nodeId < config.getClusterSize(); nodeId++ )
                {
                    if ( cluster.getNodeState( nodeId ) == NodeState.OPERATIONAL )
                    {
                        cluster.incLostJobs( nodeId );
                        var currentJob = cluster.getCurrentJob( nodeId );
                        // ASSERT: if not idle then must have had a job
                        if ( currentJob == null )
                        {
                            System.out.println();
                            System.out.println( "If node is not idle then should have had a job (EpochTimeout)" );
                            System.exit( 1 );
                        }
                        currentJob.incRetries();
                        queue.addJob( currentJob );
                        cluster.setNodeState( nodeId, NodeState.IDLE );
                    }
                }

                cluster.setCurrentEpochState( EpochState.ABORTING );
                Common.generateAbortCompletionEvent( thisEventTime, rand, eventList, currentEpoch );
            }
        }
        else
        {
            transitionToWaiting( cluster, config );

            // if all operational nodes are commit then generate a commit completed operation event
            if ( cluster.areAllOperationalNodesReadyToCommit( config ) )
            {
                eventList.addEvent( new ReadyToCommitEvent( thisEventTime, EventType.READY_TO_COMMIT, currentEpoch ) );
            }
        }
    }

    private static void transitionToWaiting( Cluster cluster, Config config )
    {
        cluster.setCurrentEpochState( EpochState.WAITING );

        // move idle nodes to ready
        for ( int i = 0; i < config.getClusterSize(); i++ )
        {
            if ( cluster.getNodeState( i ) == NodeState.IDLE )
            {
                cluster.setNodeState( i, NodeState.READY );
            }
        }
    }
}

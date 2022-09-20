package action;

import event.CommitOperationEvent;
import event.EventType;
import event.ReadyToCommitEvent;
import org.apache.log4j.Logger;
import state.Cluster;
import state.EpochState;
import utils.Common;
import utils.Config;
import utils.EventList;
import utils.Rand;

import java.util.Objects;

public class ReadyToCommitAction
{
    private final static Logger LOGGER = Logger.getLogger( ReadyToCommitAction.class.getName() );

    public static void ready( ReadyToCommitEvent event, Cluster cluster, Config config, EventList eventList, Rand rand )
    {
        var currentEpoch = cluster.getCurrentEpoch();
        var thisEventTime = event.getEventTime();

        var algorithm = config.getAlgorithm();

        if ( Objects.equals( algorithm, "multi" ) )
        {
            // compute commit groups
            cluster.computeCommitGroups();
            // mark groups with crashed nodes as failed
            cluster.transitionNodesInCrashedGroupsToFailed();

            var operationalCommitGroups = cluster.getNumberOfOperationalCommitGroups();

            if ( operationalCommitGroups > 0 )
            {
                cluster.setCurrentEpochState( EpochState.COMMITTING );
                generateCommitOperationCompletionEvent( eventList, rand, thisEventTime, currentEpoch );
            }
            else
            {
                // abort everything
                Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
            }
        }
        else
        {
            if ( cluster.epochContainsFailureEvent() )
            {
                Common.transitionToAborting( cluster, eventList, rand, thisEventTime, currentEpoch );
            }
            else
            {
                cluster.setCurrentEpochState( EpochState.COMMITTING );
                generateCommitOperationCompletionEvent( eventList, rand, thisEventTime, currentEpoch );
            }
        }
    }

    private static void generateCommitOperationCompletionEvent( EventList eventList, Rand rand, double thisEventTime, int currentEpoch )
    {
        var commitOperationEventTime = thisEventTime + rand.generateCommitOperationDuration();
        var commitOperationEvent = new CommitOperationEvent( commitOperationEventTime, EventType.COMMIT_COMPLETED, currentEpoch );
        eventList.addEvent( commitOperationEvent );
    }
}

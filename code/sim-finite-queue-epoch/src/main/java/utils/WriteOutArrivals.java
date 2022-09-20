package utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class WriteOutArrivals
{

    public static void writeOutArrivals( Config config, Metrics metrics )
    {

        var queue = metrics.getPerEpochArrivals();

        StringBuilder sb = new StringBuilder();
        for ( int i = 0; i < queue.size(); i++ )
        {
            sb.append( queue.get( i ) );
            if ( i != queue.size() - 1 )
            {
                sb.append( "," );
            }
        }

        String format = String.format( "%s", sb );

        BufferedWriter outputStream = null;
        FileWriter fileWriter;
        try
        {
            File file = new File( "perEpochArrivals.csv" );
            if ( !file.exists() )
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                outputStream.append( format );
                outputStream.append( "\n" );
            }
            else
            {
                fileWriter = new FileWriter( file, true );
                outputStream = new BufferedWriter( fileWriter );
                try
                {
                    outputStream.append( format );
                    outputStream.append( "\n" );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace();
        }
        finally
        {
            if ( outputStream != null )
            {
                try
                {
                    outputStream.flush();
                    outputStream.close();
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                }
            }
        }
    }
}
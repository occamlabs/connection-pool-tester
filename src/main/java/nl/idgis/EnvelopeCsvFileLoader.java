package nl.idgis;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.deegree.geometry.Envelope;
import org.deegree.geometry.GeometryFactory;

public class EnvelopeCsvFileLoader {

    public List<Envelope> load( final File csvFile )
                            throws IOException {
        final List<String> rawLines = FileUtils.readLines( csvFile );
        final List<String> trimmedLines = new ArrayList<String>();
        for ( final String rawLine : rawLines ) {
            final String trimmedLine = rawLine.trim();
            if ( !trimmedLine.isEmpty() ) {
                trimmedLines.add( trimmedLine );
            }
        }
        if ( rawLines.size() < 2 ) {
            throw new IOException( "CSV file is expected to contain at least two non-empty lines." );
        }
        final List<Envelope> boxes = new ArrayList<Envelope>();
        for ( int i = 1; i < trimmedLines.size(); i++ ) {
            final Envelope box = buildEnvelope( trimmedLines.get( i ) );
            boxes.add( box );
        }
        return boxes;
    }

    private Envelope buildEnvelope( final String line )
                            throws IllegalArgumentException {
        final String[] values = line.split( ";" );
        if ( values.length != 6 ) {
            throw new IllegalArgumentException( "Wrong number of CSV values. Expected 6, found: " + values.length );
        }
        final double west28992 = Double.parseDouble( values[2] );
        final double east28992 = Double.parseDouble( values[3] );
        final double south28992 = Double.parseDouble( values[4] );
        final double north28992 = Double.parseDouble( values[5] );
        final GeometryFactory factory = new GeometryFactory();
        return factory.createEnvelope( west28992, south28992, east28992, north28992, null );
    }

}

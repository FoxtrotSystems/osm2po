/*
   Copyright (c) 2014 Carsten Moeller, Pinneberg, Germany. <info@osm2po.de>

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU Lesser General Public License as 
   published by the Free Software Foundation, either version 3 of the 
   License, or (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
   
   $Id: PgRoutingWriter.java 4035 2015-12-22 13:32:22Z carsten $
*/

package io.foxtrot.osm2po.postp;

import de.cm.osm2po.Config;
import de.cm.osm2po.Version;
import de.cm.osm2po.converter.PostProcessor;
import de.cm.osm2po.converter.Segmenter;
import de.cm.osm2po.logging.Log;
import de.cm.osm2po.model.LatLons;
import de.cm.osm2po.model.Node;
import de.cm.osm2po.model.SegmentedWay;
import de.cm.osm2po.model.WaySegment;
import de.cm.osm2po.primitives.InStream;
import de.cm.osm2po.primitives.InStreamDisk;
import de.cm.osm2po.primitives.VarTypeDesk;
import org.geotools.referencing.GeodeticCalculator;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;

import static de.cm.osm2po.misc.Utils.DF;
import static de.cm.osm2po.misc.Utils.bytesToHex;
import static de.cm.osm2po.misc.Utils.roundE7;
import static de.cm.osm2po.misc.WkbUtils.toLineWkb;
import static de.cm.osm2po.misc.WkbUtils.toMultiLineWkb;
import static de.cm.osm2po.primitives.VarString.toUTF8QuotedSqlBytes;

public class PgRoutingWriter implements PostProcessor {

    private static final byte KOMMA = ',';
    private static final byte SEMIKOLON = ';';
    private static final byte NEWLINE = '\n';

    private boolean asMultilineString;

    private double computeDistanceKm(WaySegment segment) {
        double meters = 0.0D;

        Node[] nodes = segment.getNodes();
        GeodeticCalculator distanceCalc = new GeodeticCalculator();

        if (nodes.length > 1) {
            Node ndFrom = nodes[0];

            for (int i = 1; i < nodes.length; ++i) {
                Node ndTo = nodes[i];
                distanceCalc.setStartingGeographicPoint(ndFrom.getLon(), ndFrom.getLat());
                distanceCalc.setDestinationGeographicPoint(ndTo.getLon(), ndTo.getLat());
                meters += distanceCalc.getOrthodromicDistance();
                ndFrom = ndTo;
            }
        }

        return meters / 1000.0;
    }

    @Override
    public void run(Config config, int index) throws Exception {

        File dir = config.getWorkDir();
        String prefix = config.getPrefix();
        Log log = config.getLog();

        String key = "postp." + index + ".writeMultiLineStrings";
        this.asMultilineString = Boolean.valueOf(
                config.getProperty(key, "false"));

        String tableName = (prefix + "_2po_4pgr").toLowerCase(); // Postgres-Problem
        String fileName = prefix + "_2po_4pgr.sql";

        File waysInFile = new File(dir, Segmenter.SEGMENTS_FILENAME);
        if (!waysInFile.exists()) {
            log.error("File not found : " + waysInFile);
            return;
        }
        InStream inStream = new InStreamDisk(waysInFile);

        OutputStream os = null;
        File sqlOutFile = null;
        if (config.isPipeOut()) {
            os = System.out;
            log.info("Writing results to stdout");
        } else {
            sqlOutFile = new File(dir, fileName);
            os = new BufferedOutputStream(
                    new FileOutputStream(sqlOutFile), 0x10000 /*64k*/);
            log.info("Creating sql file " + sqlOutFile.toString());
        }

        byte varType = inStream.readByte();
        if (varType != VarTypeDesk.typeIdOf(SegmentedWay.class))
            throw new RuntimeException("Unexpected VarType " + varType);

        String lineformat = this.asMultilineString ? "MULTILINESTRING" : "LINESTRING";
        os.write((""
                + "-- Created by  : " + Version.getName() + "\n"
                + "-- Version     : " + Version.getVersion() + "\n"
                + "-- Author (c)  : Carsten Moeller - info@osm2po.de\n"
                + "-- Date        : " + new Date().toString()
                + "\n\n"
                + "SET client_encoding = 'UTF8';"
                + "\n\n"
                + "DROP TABLE IF EXISTS " + tableName + ";"
                + "\n"
                + "-- SELECT DropGeometryTable('" + tableName + "');"
                + "\n\n"
                + "CREATE TABLE " + tableName + "("
                + "id integer, osm_id bigint, "
                + "osm_name character varying, osm_meta character varying, "
                + "osm_source_id bigint, osm_target_id bigint, "
                + "clazz integer, flags integer, "
                + "source integer, target integer, "
                + "km double precision, kmh integer, "
                + "cost double precision, reverse_cost double precision, "
                + "x1 double precision, y1 double precision, "
                + "x2 double precision, y2 double precision"
                + ");\n"
                + "SELECT AddGeometryColumn('" + tableName + "', "
                + "'geom_way', 4326, '" + lineformat + "', 2);\n")
                         .getBytes());

        byte[] INSERT = ("\nINSERT INTO " + tableName + " VALUES ").getBytes();

        SegmentedWay way = new SegmentedWay();
        long n = 0, g = 0;
        while (!inStream.isEof()) {
            way.readFromStream(inStream);

            long osm_id = way.getId();
            byte clazz = way.getClazz();
            int flags = way.getFlags();
            byte[] nameSql = toUTF8QuotedSqlBytes(way.getName(), true);
            byte[] metaSql = toUTF8QuotedSqlBytes(way.getMeta(), true);

            int kmh = way.getKmh();
            if (kmh <= 0) kmh = 1;
            boolean isOneWay = way.isOneWay();

            for (int i = 0; i < way.getSegments().length; i++) {
                WaySegment waySegment = way.getSegments()[i];
                int id = waySegment.getId();
                int source = waySegment.getSourceId();
                int target = waySegment.getTargetId();
                Node n1 = waySegment.getNodes()[0];
                Node n2 = waySegment.getNodes()[waySegment.getNodes().length - 1];
                long osmSourceId = n1.getId();
                long osmTargetId = n2.getId();
                double km = roundE7(computeDistanceKm(waySegment));
                double cost = roundE7(km / kmh);
                double reverse_cost = isOneWay ? 1000000d : cost;

                LatLons line = new LatLons().setCoords(waySegment.getNodes());
                String geom_way = this.asMultilineString
                        ? bytesToHex(toMultiLineWkb(new LatLons[]{line}))
                        : bytesToHex(toLineWkb(line));

                // Building an insert group of 25 records turns out to be much faster
                if (g == 25) {
                    os.write(SEMIKOLON);
                    os.write(NEWLINE);
                    g = 0;
                }
                if (++g == 1) os.write(INSERT);
                else os.write(KOMMA);
                os.write(NEWLINE);

                os.write(("(" + id + ", " + osm_id + ", ").getBytes());
                os.write(nameSql);
                os.write(", ".getBytes());
                os.write(metaSql);
                os.write((", "
                        + osmSourceId + ", " + osmTargetId + ", "
                        + clazz + ", " + flags + ", "
                        + source + ", " + target + ", "
                        + km + ", " + kmh + ", " + cost + ", " + reverse_cost + ", "
                        + n1.getLon() + ", " + n1.getLat() + ", "
                        + n2.getLon() + ", " + n2.getLat() + ", "
                        + "'" + geom_way + "'" + ")")
                                 .getBytes());

                if (++n % 50000 == 0) log.progress(DF(n) + " Segments written.");
            }
        }

        os.write(SEMIKOLON);
        os.write(NEWLINE);

        log.info(DF(n) + " Segments written.");

        os.write(("\n"
                + "ALTER TABLE " + tableName + " ADD CONSTRAINT pkey_" + tableName + " PRIMARY KEY(id);\n"
                + "CREATE INDEX idx_" + tableName + "_source ON " + tableName + "(source);\n"
                + "CREATE INDEX idx_" + tableName + "_target ON " + tableName + "(target);\n"
                + "-- CREATE INDEX idx_" + tableName + "_osm_source_id ON " + tableName + "(osm_source_id);\n"
                + "-- CREATE INDEX idx_" + tableName + "_osm_target_id ON " + tableName + "(osm_target_id);\n"
                + "-- CREATE INDEX idx_" + tableName + "_geom_way ON " + tableName
                + " USING GIST (geom_way);\n")
                         .getBytes());

        inStream.close();

        if (sqlOutFile != null) {
            os.close();
            log.info("commandline template:\n"
                             + "psql -U [username] -d [dbname] -q -f \""
                             + sqlOutFile.getAbsolutePath() + "\"");
        }

    }

}



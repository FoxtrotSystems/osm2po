/* 
   Copyright (c) 2011 Carsten Moeller, Pinneberg, Germany. <info@osm2po.de>

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
   
   $Id: PgVertexWriter.java 3847 2015-07-10 19:05:38Z cmindividual $
*/

package io.foxtrot.osm2po.postp;

import de.cm.osm2po.Config;
import de.cm.osm2po.Version;
import de.cm.osm2po.converter.PostProcessor;
import de.cm.osm2po.converter.Segmenter;
import de.cm.osm2po.logging.Log;
import de.cm.osm2po.model.Restriction;
import de.cm.osm2po.model.Vertex;
import de.cm.osm2po.primitives.InStream;
import de.cm.osm2po.primitives.InStreamDisk;
import de.cm.osm2po.primitives.VarTypeDesk;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.Date;

import static de.cm.osm2po.misc.Utils.DF;
import static de.cm.osm2po.misc.Utils.bytesToHex;
import static de.cm.osm2po.misc.WkbUtils.toPointWkb;
import static de.cm.osm2po.primitives.VarString.toUTF8QuotedSqlBytes;

public class PgVertexWriter implements PostProcessor {

    @Override
    public void run(Config config, int index) throws Exception {

        File dir = config.getWorkDir();
        String prefix = config.getPrefix();
        Log log = config.getLog();

        String tableName = (prefix + "_2po_vertex").toLowerCase(); // Postgres-Problem
        String fileName = prefix + "_2po_vertex.sql";

        File vertexInFile = new File(dir, Segmenter.VERTICES_FILENAME);
        if (!vertexInFile.exists()) {
            log.error("File not found : " + vertexInFile);
            return;
        }

        InStream inStream = new InStreamDisk(vertexInFile);

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
        if (varType != VarTypeDesk.typeIdOf(Vertex.class))
            throw new RuntimeException("Unexpected VarType " + varType);

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
                + "id integer, clazz integer, osm_id bigint, "
                + "osm_name character varying, ref_count integer, "
                + "restrictions character varying"
                + ");\n"
                + "SELECT AddGeometryColumn('" + tableName + "', "
                + "'geom_vertex', 4326, 'POINT', 2);")
                         .getBytes());

        Vertex vertex = new Vertex();
        long n = 0, g = 0;
        while (!inStream.isEof()) {
            vertex.readFromStream(inStream);

            int id = vertex.getId();
            long osmId = vertex.getOsmId();
            byte clazz = vertex.getClazz();
            int nRefs = vertex.getRefCounter();
            byte[] nameSql = toUTF8QuotedSqlBytes(vertex.getOsmName(), true);

            String geom_vertex = bytesToHex(toPointWkb(vertex));
            String vnrOrNull = "null";
            if (vertex.getRestrictions() != null) {
                vnrOrNull = "'";
                for (Restriction vnr : vertex.getRestrictions()) {
                    if ((vnr.getClazz() & 1) != 0) {
                        vnrOrNull += "-";
                    } else {
                        vnrOrNull += "+";
                    }
                    vnrOrNull += vnr.getFrom() + "_" + vnr.getTo();
                }
                vnrOrNull += "'";
            }

            // MultiInsert is faster.
            if (g == 50) {
                os.write(";".getBytes());
                g = 0;
            }
            if (++g == 1) {
                os.write(("\n\nINSERT INTO "
                        + tableName + " VALUES ").getBytes());
            } else {
                os.write(",".getBytes());
            }

            os.write(("\n(" + id + ", "
                    + clazz + ", " + osmId + ", ").getBytes());
            os.write(nameSql);
            os.write((", " + nRefs + ", " + vnrOrNull + ", "
                    + "'" + geom_vertex + "'" + ")").getBytes());

            if (++n % 50000 == 0) log.progress(DF(n) + " Vertices written.");
        }

        os.write(";".getBytes());

        log.info(DF(n) + " Vertices written.");

        os.write(("\n\n"
                + "ALTER TABLE " + tableName + " ADD CONSTRAINT pkey_" + tableName + " PRIMARY KEY(id);\n"
                + "CREATE INDEX idx_" + tableName + "_osm_id ON " + tableName + "(osm_id);\n"
                + "-- CREATE INDEX idx_" + tableName + "_geom_vertex ON " + tableName
                + " USING GIST (geom_vertex);\n").getBytes());

        inStream.close();

        if (sqlOutFile != null) {
            os.close();
            log.info("commandline template:\n"
                             + "psql -U [username] -d [dbname] -q -f \""
                             + sqlOutFile.getAbsolutePath() + "\"");
        }
    }

}

#!/usr/bin/env bash
# Usage osm2po.sh ${country_code} ${osm_pbf_file}
# Parses a osm.pbf file and compile a pgrouting compatible format as a sql file.
# Usage: ./osm2po.sh ${country_code} osm.pbf

args=("$@")
country_code=${args[0]}
osm_file=${args[1]}
output_sql_file=${country_code}_osm.sql

mvn clean package

printf "CREATE EXTENSION IF NOT EXISTS POSTGIS;
CREATE EXTENSION IF NOT EXISTS PGROUTING;\n\n
" > ${output_sql_file}
java -cp ./lib/osm2po-core-5.1.0-signed.jar:./target/foxtrot-osm2po-plugins-1.0-jar-with-dependencies.jar de.cm.osm2po.Main cmd=tjsp prefix=${country_code} postp.0.class=io.foxtrot.osm2po.postp.PgRoutingWriter postp.1.class=io.foxtrot.osm2po.postp.PgVertexWriter ${osm_file} >> ${output_sql_file}
rm -rf ${country_code}

printf "\n\nSELECT pgr_createTopology('${country_code}_osm_edges', '0.000001', 'geom_way');
ALTER TABLE ${country_code}_osm_edges_vertices_pgr add column the_geog GEOGRAPHY(POINT, 4326);
UPDATE ${country_code}_osm_edges_vertices_pgr set the_geog = the_geom::geography;
CREATE INDEX idx_${country_code}_osm_edges_vertices_pgr_the_geog ON ${country_code}_osm_edges_vertices_pgr USING GIST(the_geog);
" >> ${output_sql_file}

printf "You can now run: psql DB_NAME -f ${country_code}_osm.sql"

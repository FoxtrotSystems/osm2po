#!/usr/bin/env bash
# Usage osm2po.sh ${country_code} ${osm_pbf_file}
# Parses a osm.pbf file and compile a pgrouting compatible format as a sql file.
# Usage: ./osm2po.sh ${country_code} osm.pbf

args=("$@")
country_code=${args[0]}
osm_file=${args[1]}

mvn clean package
java -cp ./lib/osm2po-core-5.1.0-signed.jar:./target/foxtrot-osm2po-plugins-1.0-jar-with-dependencies.jar de.cm.osm2po.Main cmd=tjsp prefix=${country_code} postp.0.class=io.foxtrot.osm2po.postp.PgRoutingWriter postp.1.class=io.foxtrot.osm2po.postp.PgVertexWriter ${osm_file} | tee ${country_code}_osm.sql
rm -rf ${country_code}
printf "Run these commands
1. psql DB_NAME -c 'create extension postgis'
2. psql DB_NAME -c 'create extension pgrouting'
3. psql DB_NAME -f ${country_code}_osm.sql
4. psql DB_NAME
5. select pgr_createTopology('${country_code}_osm_edges', '0.000001', 'geom_way');
6. alter table ${country_code}_osm_edges_vertices_pgr add column the_geog GEOGRAPHY(POINT, 4326);
7. update ${country_code}_osm_edges_vertices_pgr set the_geog = the_geom::geography;
8. create index idx_${country_code}_osm_edges_vertices_pgr_the_geog ON ${country_code}_osm_edges_vertices_pgr USING GIST(the_geog);
9. select pgr_analyzeGraph('${country_code}_osm_edges', '0.000001', 'geom_way');
"

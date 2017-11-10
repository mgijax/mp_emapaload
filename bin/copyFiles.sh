#!/bin/sh

echo 'copying bhmgiapp01:/data/loads/mgi/vocload/runTimeMP/mp.owl to /data/loads/mgi/vocload/runTimeMP'
cd /data/loads/mgi/vocload/runTimeMP
scp bhmgiapp01:/data/loads/mgi/vocload/runTimeMP/mp.owl .

echo 'copying  bhmgiapp01:/data/loads/mgi/vocload/emap/input/EMAPA.obo /data/loads/mgi/vocload/emap/input'
cd /data/loads/mgi/vocload/emap/input/
scp bhmgiapp01:/data/loads/mgi/vocload/emap/input/EMAPA.obo .

echo 'copying bhmgiapp01:/data/downloads/purl.obolibrary.org/obo/uberon.obo /data/downloads/purl.obolibrary.org/obo'
cd /data/downloads/purl.obolibrary.org/obo
scp bhmgiapp01:/data/downloads/purl.obolibrary.org/obo/uberon.obo .

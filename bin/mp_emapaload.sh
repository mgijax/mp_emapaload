#!/bin/sh

#
# This script is a wrapper around the process that loads 
# MP/EMAPA relationships
#
#
#     mp_emapaload.sh 
#

cd `dirname $0`/..
CONFIG_LOAD=`pwd`/mp_emapaload.config

cd `dirname $0`
LOG=`pwd`/mp_emapaload.log
rm -rf ${LOG}

USAGE='Usage: mp_emapaload.sh'
SCHEMA='mgd'

#
#  Verify the argument(s) to the shell script.
#
if [ $# -ne 0 ]
then
    echo ${USAGE} | tee -a ${LOG}
    exit 1
fi

#
# verify & source the configuration file
#

if [ ! -r ${CONFIG_LOAD} ]
then
    echo "Cannot read configuration file: ${CONFIG_LOAD}"
    exit 1
fi

. ${CONFIG_LOAD}

#
# Just a verification of where we are at
#

echo "MGD_DBSERVER: ${MGD_DBSERVER}"
echo "MGD_DBNAME: ${MGD_DBNAME}"

#
#  Source the DLA library functions.
#

if [ "${DLAJOBSTREAMFUNC}" != "" ]
then
    if [ -r ${DLAJOBSTREAMFUNC} ]
    then
        . ${DLAJOBSTREAMFUNC}
    else
        echo "Cannot source DLA functions script: ${DLAJOBSTREAMFUNC}" | tee -a ${LOG}
        exit 1
    fi
else
    echo "Environment variable DLAJOBSTREAMFUNC has not been defined." | tee -a ${LOG}
    exit 1
fi

#
# verify input file exists and is readable
#

if [ ! -r ${INPUT_FILE_DEFAULT} ]
then
    # set STAT for endJobStream.py
    STAT=1
    checkStatus ${STAT} "Cannot read from input file: ${INPUT_FILE_DEFAULT}"
fi

#
# createArchive including OUTPUTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#

preload ${OUTPUTDIR}

#
# rm all files/dirs from OUTPUTDIR
#
#cleanDir ${OUTPUTDIR}

#
# run the load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run mp_emapaload.py"  | tee -a ${LOG_DIAG}
${MPEMAPALOAD}/bin/mp_emapaload.py  
STAT=$?
checkStatus ${STAT} "${MPEMPALOAD}/bin/mp_emapaload.py"

#
# Archive a copy of the input file, adding a timestamp suffix.
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Archive input files" >> ${LOG_DIAG}
TIMESTAMP=`date '+%Y%m%d.%H%M'`
ARC_FILE=`basename ${INPUT_FILE_MP}`.${TIMESTAMP}
cp -p ${INPUT_FILE_MP} ${ARCHIVEDIR}/${ARC_FILE}
ARC_FILE=`basename ${INPUT_FILE_UBERON}`.${TIMESTAMP}
cp -p ${INPUT_FILE_UBERON} ${ARCHIVEDIR}/${ARC_FILE}
ARC_FILE=`basename ${INPUT_FILE_EMAPA}`.${TIMESTAMP}
cp -p ${INPUT_FILE_EMAPA} ${ARCHIVEDIR}/${ARC_FILE}

# run postload cleanup and email logs

shutDown


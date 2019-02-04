#!/bin/sh

#
# This script is a wrapper around the process that loads 
# MP/EMAPA relationships
#
#
#     mp_emapaload.sh 
#

cd `dirname $0` 

if [ "${MGICONFIG}" = "" ]
then
    MGICONFIG=/usr/local/mgi/live/mgiconfig
    export MGICONFIG
fi

. ${MGICONFIG}/master.config.sh

CONFIG_LOAD=${MPEMAPALOAD}/mp_emapaload.config
LOG=${MPEMAPALOAD}/mp_emapaload.log
rm -rf ${LOG}

USAGE='Usage: mp_emapaload.sh'

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
# createArchive including OUTPUTDIR, RPTDIR, startLog, getConfigEnv
# sets "JOBKEY"
#

preload ${OUTPUTDIR} ${RPTDIR}

#
# rm all files/dirs from OUTPUTDIR
#
#cleanDir ${OUTPUTDIR}

#
# save current report
#
cp -r ${PUBREPORTDIR}/output/MP_EMAPA.rpt ${RPTDIR}/MP_EMAPA.rpt.previous

#
# run the load
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Run mp_emapaload.py"  | tee -a ${LOG_DIAG}
${MPEMAPALOAD}/bin/mp_emapaload.py  
STAT=$?
checkStatus ${STAT} "${MPEMAPALOAD}/bin/mp_emapaload.py"

# run MP_EMAPA report, save in ${RPTDIR} directory, and diff previous with new
# send results to ${MAIL_LOG_CUR}
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Creating new MP_EMAPA.rpt report" >> ${LOG_DIAG}
REPORTOUTPUTDIR=${PUBREPORTDIR}/output
REPORTLOGSDIR=${PUBREPORTDIR}/logs
export REPORTOUTPUTDIR REPORTLOGSDIR
cd ${PUBRPTS}/weekly
./MP_EMAPA.py >> ${LOG_DIAG}
cp -r ${PUBREPORTDIR}/output/MP_EMAPA.rpt ${RPTDIR}/MP_EMAPA.rpt.new >> ${LOG_DIAG}
rm -rf ${LOG_DIFF} >> ${LOG_DIAG}
date >> ${LOG_DIFF}
echo "Comparing previous MP_EMAPA.rpt with new report" >> ${LOG_DIFF}
diff ${RPTDIR}/MP_EMAPA.rpt.previous ${RPTDIR}/MP_EMAPA.rpt.new >> ${LOG_DIFF}
mailx -s "${MAIL_LOADNAME} - Diff Log" `echo ${MAIL_LOG_CUR}` < ${LOG_DIFF}

#
# Archive a copy of the input file, adding a timestamp suffix.
#
echo "" >> ${LOG_DIAG}
date >> ${LOG_DIAG}
echo "Archive input files" >> ${LOG_DIAG}
TIMESTAMP=`date '+%Y%m%d.%H%M'`
ARC_FILE=`basename ${INPUT_FILE_MP}`.${TIMESTAMP}
cp  ${INPUT_FILE_MP} ${ARCHIVEDIR}/${ARC_FILE}
ARC_FILE=`basename ${INPUT_FILE_UBERON}`.${TIMESTAMP}
cp  ${INPUT_FILE_UBERON} ${ARCHIVEDIR}/${ARC_FILE}
ARC_FILE=`basename ${INPUT_FILE_EMAPA}`.${TIMESTAMP}
cp ${INPUT_FILE_EMAPA} ${ARCHIVEDIR}/${ARC_FILE}
ARC_FILE=`basename ${INPUT_FILE_EMAPA}`.${TIMESTAMP}
cp ${INPUT_FILE_EMAPA} ${ARCHIVEDIR}/${ARC_FILE}

# run postload cleanup and email logs

shutDown


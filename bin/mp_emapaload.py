#!/usr/local/bin/python
#
#  mp_emapaload.py
###########################################################################
#
#  Purpose:
#
#      Validate input and create feature relationships bcp file
#
#  Usage:
#
#      mp_emapaload.py 
#
#  Inputs:
#
#	1. MP, UBERON and EMAPA OWL files
#
#	2. Configuration - see mp_emapaload.config
#
#  Outputs:
#
#       1. MGI_Relationship.bcp
#
#  Exit Codes:
#
#      0:  Successful completion
#      1:  An exception occurred
#      2:  bcp fails

#  Assumes:
#
#	
#  Implementation:
#
#      This script will perform following steps:
#
#      1) Validate the arguments to the script.
#      2) Perform initialization steps.
#      3) Open the input/output files.
#      4) Run the QC  checks
#      5) Run the load if QC checks pass
#      6) Close the input/output files.
#      7) Delete existing relationships
#      8) BCP in new relationships:
#
#  Notes:  None
#
###########################################################################
#
#  Modification History:
#
#  Date        SE   Change Description
#  ----------  ---  -------------------------------------------------------
#
#  11/03/2017  sc  Initial development
#
###########################################################################

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
import string

import db
import mgi_utils

from xml.dom.minidom import parse
from xml import *
#
#  CONSTANTS
#
TAB = '\t'
CRT = '\n'
DATE = mgi_utils.date("%m/%d/%Y")
USAGE='mp_emapaload.py'

#
#  GLOBALS
#

# input files
inMP = os.environ['INPUT_FILE_MP']
inUBERON = os.environ['INPUT_FILE_UBERON']
inEMAPA = os.environ['INPUT_FILE_EMAPA']

# input file descriptors
fpUin = ''
fpEin = ''

# output files
outMP = os.environ['OUTPUTFILE_MTOU']
outUberon =  os.environ['OUTPUTFILE_UTOE']
outEmapa =  os.environ['OUTPUTFILE_EMAPA']

# output file descriptors
fpMtoU = ''
fpUtoE = ''
fpEmapa = ''

# output bcp files
bcpFile =   os.environ['RELATIONSHIP_BCP']
outputDir = os.environ['OUTPUTDIR']
relationshipFile = '%s/%s' % (outputDir, bcpFile)
fpRelationshipFile = ''

# MP to UBERON from MP file {mp:[uberon, ...], ...}
mpDict = {}

# UBERON to EMAPA from UBERON file {uberon:[emapa, ...], ...}
uberonDict = {} 

#  List of emapa IDs from the EMAPA file
emapaList = []

# The mp_emapa relationship category key 'mp_to_emapa'
catKey = 1005

# the mp_emapa relationship term key 'mp_to_emapa'
relKey = 17396910

# the mp_emapa qualifier key 'Not Specified'
qualKey = 11391898

# the mpo_emapa evidence key 'Not Specified'
evidKey = 17396909

# the mp_emapa reference key 'J:229957'
refsKey = 231052

# mp_emapaload user key
userKey = 1558

# database primary keys, will be set to the next available from the db
nextRelationshipKey = 1000	# MGI_Relationship._Relationship_key

# Lookups
mpHeaderLookup = {}
hpoLookup = {}

# for bcp
bcpin = '%s/bin/bcpin.csh' % os.environ['PG_DBUTILS']
server = os.environ['MGD_DBSERVER']
database = os.environ['MGD_DBNAME']
table = 'MGI_Relationship'

def checkArgs ():
    # Purpose: Validate the arguments to the script.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: exits if unexpected args found on the command line
    # Throws: Nothing

    if len(sys.argv) != 1:
        print USAGE
        sys.exit(1)
    return

# end checkArgs() -------------------------------

def init():
    # Purpose: create lookups, open files, create db connection, gets max
    #	keys from the db
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened,
    #  creates files in the file system, creates connection to a database

    global nextRelationshipKey, mpHeaderLookup, hpoLookup

    #
    # Open input and output files
    #
    openFiles()

    #
    # create database connection
    #
    user = os.environ['MGD_DBUSER']
    passwordFileName = os.environ['MGD_DBPASSWORDFILE']
    db.useOneConnection(1)
    db.set_sqlUser(user)
    db.set_sqlPasswordFromFile(passwordFileName)

    #
    # get next MGI_Relationship key
    #
    results = db.sql('''select max(_Relationship_key) + 1 as nextKey
	    from MGI_Relationship''', 'auto')
    if results[0]['nextKey'] is None:
	nextRelationshipKey = 1000
    else:
	nextRelationshipKey = results[0]['nextKey']

    #
    # create lookups
    #
    # lookup of MP header terms
    results = db.sql('''select a.accid, t.term, t._Term_key
        from DAG_Node n, VOC_Term t, ACC_Accession a
        where n._Label_key = 3
        and n._Object_key = t._Term_key
        and t._Vocab_key = 5
        and t.isObsolete = 0
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 34
        and a.preferred = 1''', 'auto')

    for r in results:
        mpId = string.lower(r['accid'])
        termKey = r['_Term_key']
        mpHeaderLookup[mpId] = termKey

    # load lookup of HPO terms
    results = db.sql('''select a.accid, t.term, t._Term_key
        from VOC_Term t, ACC_Accession a
        where t._Vocab_key = 106
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 180''', 'auto')

    for r in results:
        hpoId = string.lower(r['accid'])
        termKey = r['_Term_key']
        hpoLookup[hpoId] = termKey

    return

# end init() -------------------------------

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened, 
    #  creates files in the file system

    global fpUin, fpEin, fpMtoU, fpUtoE, fpEmapa, fpRelationshipFile

    try:
        fpUin = open(inUBERON, 'r')
    except:
        print 'Cannot open UBERON input file: %s' % inUBERON
        sys.exit(1)
    try:
	fpEin = open(inEMAPA, 'r')
    except:
        print 'Cannot open EMAPA input file: %s' % inEMAPA
        sys.exit(1)
    try:
        fpMtoU = open(outMP, 'w')
    except:
        print 'Cannot open MP/Uberon output file: %s' % inUBERON
        sys.exit(1)
    try:
        fpUtoE = open(outUberon, 'w')
    except:
        print 'Cannot open Uberon/EMAPA output file: %s' % inUBERON
        sys.exit(1)
    try:
        fpEmapa = open(outEmapa, 'w')
    except:
        print 'Cannot open EMAPA output file: %s' % inUBERON
        sys.exit(1)

    try:
        fpRelationshipFile = open(relationshipFile, 'w')
    except:
        print 'Cannot open Feature relationships bcp file: %s' % relationshipFile
        sys.exit(1)

    return

# end openFiles() -------------------------------


def closeFiles ():
    # Purpose: Close all file descriptors
    # Returns: Nothing
    # Assumes: all file descriptors were initialized
    # Effects: Nothing
    # Throws: Nothing

    global fpInFile, fpRelationshipFile

    fpUin.close()
    fpEin.close()
    fpRelationshipFile.close()

    return

# end closeFiles() -------------------------------

def createFiles( ): 
    # Purpose: parses input files, does verification
    #  creates bcp files
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: sets global variables, writes to the file system
    # Throws: Nothing

    global nextRelationshipKey
    global mpDict, uberonDict, emapaDict
    
    #
    # Iterate through input files loading data structures
    #
    print 'inMP: %s' % inMP
    # the parse function can take an open file object or a file name
    dom = parse(inMP) # dom is the complete file loaded into memory, you can view it via dom.toxml()
    print 'parsed into dom'
    for e in dom.getElementsByTagName('owl:Class'): # iterate over the NodeList, e is a single Node
	#print e.toxml()
	mpID = ''
	mpTerm = ''
	uberonID = ''
	mp = e.getAttribute('rdf:about') # the line with the MP URL
	if string.find(mp, 'MP_') == -1:
	    continue
	mpID = string.split(mp, '/')[-1]
	termNodes = e.getElementsByTagName('rdfs:label') # get the node list that contains the MP term
	mpTerm = ''
	try:
	    mpTerm = termNodes[0].firstChild.data
 	except:
	    print 'term found in position 2'
	    mpTerm = termNodes[1].firstChild.data
	if mpTerm == '':
	    print 'term not found'
	uberonNodes = e.getElementsByTagName('owl:someValuesFrom') # node list containing uberon id
	for n in uberonNodes:
	    url = n.getAttribute('rdf:resource')
	    if string.find(url, 'UBERON_') != -1:
		uberonID =  string.split(url, '/')[-1]
	print 'mpID: %s mpTerm: %s uberonID: %s' % (mpID, mpTerm, uberonID)

	# MGI_Relationship
	#fpRelationshipFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
	#    (nextRelationshipKey, TAB, catKey, TAB, objKey1, TAB, objKey2, TAB, relKey, TAB, qualKey, TAB, evidKey, TAB, refsKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

	#nextRelationshipKey += 1
    
    return

# end createFiles() -------------------------------------

def doDeletes():
    db.sql('''delete from MGI_Relationship where _CreatedBy_key = %s ''' % userKey, None)
    db.commit()
    db.useOneConnection(0)

    return

# end doDeletes() -------------------------------------

def doBcp():
    bcpCmd = '%s %s %s %s %s %s "\\t" "\\n" mgd' % (bcpin, server, database, table, outputDir, bcpFile)
    rc = os.system(bcpCmd)
    if rc <> 0:
        closeFiles()
        sys.exit(2)
    return

#####################
#
# Main
#
#####################

# check the arguments to this script, exit(1) if incorrect args
checkArgs()

# exit(1) if errors opening files
init()

# validate data and create load bcp files
createFiles()

# close all output files
closeFiles()

# delete existing relationships
#doDeletes()

# exit(2) if bcp command fails
#doBcp()
sys.exit(0)

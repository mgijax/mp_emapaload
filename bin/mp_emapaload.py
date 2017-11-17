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
#	1. MP OWL file, UBERON and EMAPA OBO files
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

# if true, write out to mp, uberon and emapa files
DEBUG = 1

# min number of records expected
MIN_RECORDS = int(os.environ['MIN_RECORDS'])

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
curLog = os.getenv('LOG_CUR')

# output file descriptors
# we will use these for debugging
fpMtoU = ''
fpUtoE = ''
fpEmapa = ''

# output bcp files
bcpFile =   os.environ['RELATIONSHIP_BCP']
outputDir = os.environ['OUTPUTDIR']
relationshipFile = '%s/%s' % (outputDir, bcpFile)
fpRelationshipFile = ''

# reporting data structures
mpNotInDatabase = []
emapaNotInDatabase = []
mpNoEmapa = []
obsAltUberonInMP = []
obsAltEmapaInUberon = []
oneMpMultiUberon = []
oneUberonMultiEmapa = []

# MP to UBERON from MP file {mp:MP relationship object, ...}
mpDict = {}

# UBERON to EMAPA from UBERON file {uberon:uberon relationship object, ...}
uberonDict = {} 

# EMAPA dict from file )emapaId:emapa relationship object with null id2
emapaDict = {}

# list of alt_id values from the uberon file
uberonAltIdList = []

# list of alt_id values from the emapa file
emapaAltIdList = []

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
mpLookup = {} # {mpID:[termKey, isObsolete], ...}
emapaLookup = {} # {emapaId:[termKey, isObsolete], ...}

class Relationship:
    # Is: data object for relationship between two vocab terms
    # Has: a set of term attributes
    # Does: provides direct access to its attributes
    #
    def __init__ (self):
        # Purpose: constructor
        # Returns: nothing
        # Assumes: nothing
        # Effects: nothing
        # Throws: nothing
        self.id1 = None # from input file
	self.preferred = None # from database
        self.term = None # from input file
	self.termKey = None # null for uberon, null if not in database for mp/emapa
        self.isObsolete = None # from uberon file or db for emapa/mp
        self.id2 = [] # list of IDs, null for emapa

# end class Relationships -----------------------------------------

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

    global nextRelationshipKey, mpLookup, emapaLookup

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
    # lookup of MP terms
    results = db.sql('''select a.accid, a.preferred, t.term, t.isObsolete, t._Term_key
        from VOC_Term t, ACC_Accession a
        where t._Vocab_key = 5
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 34
        and a.preferred = 1''', 'auto')

    for r in results:
        #mpId = string.lower(r['accid'])
	mpId = r['accid']
        termKey = r['_Term_key']
	isObsolete = r['isObsolete']
	preferred = r['preferred']
        mpLookup[mpId] = [termKey, isObsolete, preferred]

    # load lookup of EMAPA terms
    results = db.sql('''select a.accid, a.preferred, t.term, t.isObsolete, t._Term_key
        from VOC_Term t, ACC_Accession a
        where t._Vocab_key = 90
        and t._Term_key = a._Object_key
        and a._MGIType_key = 13
        and a._LogicalDB_key = 169''', 'auto')

    for r in results:
        #emapaId = string.lower(r['accid'])
	emapaId = r['accid']
        termKey = r['_Term_key']
        isObsolete = r['isObsolete']
	preferred = r['preferred']
        emapaLookup[emapaId] = [termKey, isObsolete, preferred]
    return

# end init() -------------------------------

def openFiles ():
    # Purpose: Open input/output files.
    # Returns: Nothing
    # Assumes: Nothing
    # Effects: Sets global variables, exits if a file can't be opened, 
    #  creates files in the file system

    global fpUin, fpEin, fpMtoU, fpUtoE, fpEmapa, fpRelationshipFile
    global fpLogCur

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
    try:
	fpLogCur = open(curLog, 'a')
    except:
	print 'Cannot open Curator Log file: %s' % curLog
        sys.exit(1)
    return

# end openFiles() -------------------------------


def closeFiles ():
    # Purpose: Close all file descriptors
    # Returns: Nothing
    # Assumes: all file descriptors were initialized
    # Effects: Nothing
    # Throws: Nothing

    fpUin.close()
    fpEin.close()
    fpMtoU.close() 
    fpUtoE.close()
    fpEmapa.close()

    fpRelationshipFile.close()
    fpLogCur.close()

    return

# end closeFiles() -------------------------------

def parseFiles( ): 
    # Purpose: parses input files into data structures (and intermediate files for debugging)
    # Returns: Nothing
    # Assumes: file descriptors have been initialized
    # Effects: sets global variables, writes to the file system
    # Throws: Nothing

    #global nextRelationshipKey
    #global mpDict, uberonDict, emapaDict
    
    #
    # Iterate through input files creating intermediate files nd data structures for each
    #
    parseMPFile()
    parseUberonFile()
    parseEmapaFile()
# end parseFiles() -------------------------------------

def parseMPFile():
    global mpDict, mpNotInDatabase
    # --Parse the MP owl file
    # the parse function can take an open file object or a file name
    try:
	dom = parse(inMP) # dom is the complete file loaded into memory, you can view it via dom.toxml()
    except:
	# sanity #1
	fpLogCur.write('MP OWL file not in correct format, mp_emapaload failed\n')
	sys.exit('MP OWL file not in correct format, mp_emapaload failed')
    print 'parsing MP'
    recordCt = 0
    for e in dom.getElementsByTagName('owl:Class'): # iterate over the NodeList, e is a single Node
 	recordCt += 1
	rel = Relationship()
	mpID = ''
	mpTerm = ''
	mp = e.getAttribute('rdf:about') # the line with the MP URL
	if string.find(mp, 'MP_') == -1:
	    continue
	mpID = string.split(mp, '/')[-1]
	mpID = mpID.replace('_', ':')
	termNodes = e.getElementsByTagName('rdfs:label') # get the node list that contains the MP term
	mpTerm = ''
	try:
	    mpTerm = termNodes[0].firstChild.data
 	except:
	    mpTerm = termNodes[1].firstChild.data
	    print 'term found in position 2: %s' % mpTerm
	if mpTerm == '':
	    print 'term not found'
	rel.id1 = mpID
	rel.term = mpTerm
	if mpID in mpLookup:
	    termKey, isObsolete, preferred = mpLookup[mpID]
	    #print '%s %s %s' % (mpID, termKey, isObsolete)
	    rel.termKey = termKey
	    rel.isObsolete = isObsolete
	    rel.preferred = preferred
	else:
	    # report #3
	    mpNotInDatabase.append('%s %s' % (mpID, mpTerm))
	    continue
	uberonNodes = e.getElementsByTagName('owl:someValuesFrom') # node list containing uberon id
	for n in uberonNodes:
	    url = n.getAttribute('rdf:resource')
	    if string.find(url, 'UBERON_') != -1:
		uberonID =  string.split(url, '/')[-1]
		uberonID =  uberonID.replace('_', ':')
		if uberonID not in rel.id2: # don't want duplicates
		    rel.id2.append(uberonID)
	mpDict[mpID] = rel
    # write out to file for DEBUG
    print 'MP records: %s' % len(mpDict)
    if len(mpDict) < MIN_RECORDS:
	# sanity #2
	fpLogCur.write('MP File has less than the configured minimum records: %s, number of records: %s\n' % (MIN_RECORDS, len(mpDict)))
        sys.exit('MP File has less than the configured minimum records: %s, number of records: %s' % (MIN_RECORDS, len(mpDict)))
    if DEBUG:
        keys = mpDict.keys()
        keys.sort()
        for key in keys:
	    rel = mpDict[key]
	    mpID = rel.id1
	    preferred = rel.preferred
	    mpTerm = rel.term
	    isObsolete = rel.isObsolete
	    uberonIds = string.join(rel.id2, ', ')
	    fpMtoU.write('%s%s%s%s%s%s%s%s%s%s' % (mpID, TAB, preferred, TAB, mpTerm, TAB, isObsolete, TAB, uberonIds, CRT))
# end parseMPFile() -------------------------------------

def parseUberonFile():
    global uberonDict
    # --Parse the UBERON obo file

    nameValue = 'name:'
    altIdValue = 'alt_id:'
    recordFound = 0

    # uberon specific 
    uberonIdValue = 'id: UBERON:'
    emapaXrefValue = 'xref: EMAPA:'

    emapaList = [] 
    uberonId = ''
    uberonName = ''
    isObsolete = 0
    firstRecord = 1
    print 'parsing uberon'
    lines = fpUin.readlines()
    print 'uberon lines[0]: "%s"' % string.strip(lines[0])
    if string.strip(lines[0]) != 'format-version: 1.2':
	# sanity #1
	fpLogCur.write('Uberon OBO file not in correct format, mp_emapload failed\n')
	sys.exit('Uberon OBO file not in correct format, mp_emapload failed')
    recordCt = 0	
    for line in lines:
	#print 'line: %s' % line
	if string.find(line,'[Term]') == 0:
	    recordCt += 1
	    recordFound = 0 
	    #print 'New term found'
	    if firstRecord != 1:
		rel = Relationship()
		rel.id1 = uberonId
		rel.term = uberonName
		rel.isObsolete = isObsolete
		rel.id2 = emapaList    
		uberonDict[uberonId] = rel
	    if firstRecord == 1:
		firstRecord =  0
	elif line[:11] == uberonIdValue:
	    recordFound = 1
	    emapaList = []
	    uberonId = line[4:-1]
	    #print 'uberonId: %s' % uberonId
	elif recordFound and line[:5] == nameValue:
	    uberonName = line[6:-1]
	    #print 'uberonName: %s' % uberonName
	    if uberonName.find('obsolete') == 0:
		isObsolete = 1
	    else:
		isObsolete = 0
	    #print 'isObsolete: %s' % isObsolete
	elif recordFound and line[:12] == emapaXrefValue:

	    emapaId = line[6:-1]
	    # one record like this: xref: EMAPA:35128 {source="MA"}
	    emapaId = emapaId.split(' ')[0]
	    #print 'emapaId: %s' % emapaId
	    if emapaId not in emapaList: # don't want duplicates
		emapaList.append(emapaId)
	    #if emapaId.find('RETIRED') != -1:
	    #    print 'this emapaId is retired, continuing'
	    #    recordFound = 0
	    #    continue
        elif recordFound and line[:7] == altIdValue:
	    altId = line[8:-1] 
	    uberonAltIdList.append(altId)
	else: # we don't care about this line, go to the next line
	    continue
    print 'uberonAltIdList length: %s' % len(uberonAltIdList)
    print 'uberon records: %s' % len(uberonDict)
    if len(uberonDict) < MIN_RECORDS:
	# sanity #2
	fpLogCur.write('UBERON File has less than the configured minimum records: %s, number of records: %s\n' % (MIN_RECORDS, len(uberonDict)))
	sys.exit('UBERON File has less than the configured minimum records: %s, number of records: %s' % (MIN_RECORDS, len(uberonDict)))
    if DEBUG:
	keys = uberonDict.keys()
	keys.sort()
	for key in keys:
	    rel = uberonDict[key]
	    uberonID = rel.id1
	    uberonTerm = rel.term
	    isObsolete = rel.isObsolete
	    emapaIds = string.join(rel.id2, ', ')
	    fpUtoE.write('%s%s%s%s%s%s%s%s' % (uberonID, TAB, uberonTerm, TAB, isObsolete, TAB, emapaIds, CRT))

# end parseUberonFile() -------------------------------------

def parseEmapaFile():
    global emapaDict
    # --Parse the EMAPA obo file
    print 'parsing EMAPA'
    emapaIdValue = 'id: EMAPA:'
    altIdValue = 'alt_id:'

    recordFound = 0
    nameValue = 'name:'
    lines = fpEin.readlines()
    print 'emapa lines[0]: %s' % string.strip(lines[0])
    if string.strip(lines[0]) != 'format-version: 1.2':
	# sanity #1
	fpLogCur.write('EMAPA OBO file not in correct format, mp_emapload failed\n')
        sys.exit('EMAPA OBO file not in correct format, mp_emapaload failed')

    recordCt = 0
    for line in lines:
	if line == '[Term]':
	    recordCt += 1
	    recordFound = 0

	elif line[:10] == emapaIdValue:
	    emapaId = line[4:-1]
	    #print emapaId
	    recordFound = 1
	elif recordFound and line[:7] == altIdValue:
            altId = line[8:-1]
            emapaAltIdList.append(altId)
	elif recordFound and line[:5] == nameValue:
	    emapaTerm = line[5:-1]
	    rel = Relationship()
	    rel.id1 = emapaId
	    rel.term = emapaTerm
	    termKey = 0
	    isObsolete = 0
	    preferred = 0
	    if emapaId in emapaLookup:
		termKey, isObsolete,preferred = emapaLookup[emapaId]
	    elif emapaId != 'EMAPA:0':
		# report #4
		emapaNotInDatabase.append('%s %s' % (emapaId, emapaTerm))		
		continue
	    rel.termKey = termKey
	    rel.isObsolete = isObsolete
	    rel.preferred = preferred
	    emapaDict[emapaId] = rel
    print 'emapaAltIdList length: %s' % len(emapaAltIdList) 
    print 'emapa records: %s' % len(emapaDict)
    if len(emapaDict) < MIN_RECORDS:
	# sanity #2
	fpLogCur.write('EMAPA File has less than the configured minimum records: %s, number of records: %s\n' % (MIN_RECORDS, len(emapaDict)))
        sys.exit('EMAPA File has less than the configured minimum records: %s, number of records: %s' % (MIN_RECORDS, len(emapaDict)))
    if DEBUG:
        keys = emapaDict.keys()
        keys.sort()
        for key in keys:
	    rel = emapaDict[key]
	    emapaId = rel.id1
	    preferred = rel.preferred
            emapaTerm = rel.term
            isObsolete = rel.isObsolete
	    fpEmapa.write('%s%s%s%s%s%s%s%s' % (emapaId, TAB, preferred, TAB, emapaTerm, TAB, isObsolete, CRT))


    # MGI_Relationship
    #fpRelationshipFile.write('%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s%s' % \
    #    (nextRelationshipKey, TAB, catKey, TAB, objKey1, TAB, objKey2, TAB/, relKey, TAB, qualKey, TAB, evidKey, TAB, refsKey, TAB, userKey, TAB, userKey, TAB, DATE, TAB, DATE, CRT))

    #nextRelationshipKey += 1
    
    return

# end parseEMapaFile() -------------------------------------

def findRelationships():
    # iterate thru the MP records and get their Uberon associations
    for mpId in mpDict:
	mpRel = mpDict[mpId]
	uberonList = mpRel.id2
	if len(uberonList) > 1:
	    # report #8 and load
	    print '%s: %s' % (mpId, string.join(uberonList, ', '))
	    oneMpMultiUberon.append('%s associated with multiple ubId: %s' % (mpId, string.join(uberonList, ', '))) 
	for ubId in uberonList:
	    if ubId not in uberonDict:
	 	# report and skip #5 MP that don't map to emapa - no mp to uberon
		print '%s ubId not in uberon file: %s' % (mpId, ubId)
	        mpNoEmapa.append('%s %s' % (mpId, ubId))	
	    elif uberonDict[ubId].isObsolete:
		# report and skip #6 and skip, is obsolete
		print '%s ubId is obsolete: %s' % (mpId, ubId)
		obsAltUberonInMP.append('%s ubId is obsolete: %s' % (mpId, ubId))
	    elif ubId in uberonAltIdList:
		# report and skip #6 ubid is alt_id
		print '%s ubId is alternate id: %s' % (mpId, ubId)
		obsAltUberonInMP.append('%s ubId is alternate id: %s' % (mpId, ubId))
	    else:
		print '%s in uberon file and not obsolete or alt_id: %s' % (mpId, ubId)
		uberonRel = uberonDict[ubId]
		emapaList = uberonRel.id2
		if len(emapaList) > 1:
		    # report and load #9 report
		    print '%s: %s' % (ubId, string.join(emapaList, ', '))
		    oneUberonMultiEmapa.append('%s associated with multiple emapaId: %s' % (ubId, string.join(emapaList, ', ')))
		for emapaId in emapaList:
		    if emapaId not in emapaDict:
			# report and skip #5  uberon that don't map to emapa - no uberon to emapa
			print '%s %s emapaId not in emapa file: %s' % (mpId, ubId, emapaId)
			mpNoEmapa.append('%s %s %s' % (mpId, ubId, emapaId))
		    elif emapaDict[emapaId].isObsolete:
			# report and skip #7 emapa is obsolete
			print '%s emapaId is obsolete: %s' % (ubId, emapaId)
			obsAltEmapaInUberon.append('%s emapaId is obsolete: %s' % (ubId, emapaId))
		    elif emapaId in emapaAltIdList:
			# report and skip #7 emapa is alt_id
			print  '%s emapaId is alternate id: %s' % (ubId, emapaId)
			obsAltEmapaInUberon.append('%s emapaId is alternate id: %s' % (ubId, emapaId))
		    else:
			# load this mpId/emapaId relationship
			print  '%s in emapa file and not obsolete or alt_id: %s' % (ubId, emapaId)
			print 'load %s to %s relationship' % (mpId, emapaId)
def writeQC():
    # #3
    if mpNotInDatabase:
	fpLogCur.write('MP Terms in the MP OWL file not in the database\n')
	fpLogCur.write('-' * 60 + '\n')
	fpLogCur.write(string.join(mpNotInDatabase, CRT))
	fpLogCur.write('%s%s' % (CRT, CRT))
    # #4
    if emapaNotInDatabase:
	fpLogCur.write('EMAPA Terms in the EMAPA OBO file not in the database\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(emapaNotInDatabase, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    # #5  
    if mpNoEmapa:
	fpLogCur.write('MP Terms that do not map to EMAPA\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(mpNoEmapa, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    # #6
    if obsAltUberonInMP:
        fpLogCur.write('Obsolete or Alt Uberon Terms in the MP File\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(obsAltUberonInMP, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    # #7
    if obsAltEmapaInUberon:
        fpLogCur.write('Obsolete or Alt EMAPA Terms in the Uberon File\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(obsAltEmapaInUberon, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    # #8
    if oneMpMultiUberon:
        fpLogCur.write('MP Terms that Map to Multiple Uberon Terms\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(oneMpMultiUberon, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    # #9
    if oneUberonMultiEmapa:
        fpLogCur.write('Uberon Terms that Map to Multiple EMAPA Terms\n')
        fpLogCur.write('-' * 60 + '\n')
        fpLogCur.write(string.join(oneUberonMultiEmapa, CRT))
        fpLogCur.write('%s%s' % (CRT, CRT))
    
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
parseFiles()

# find the transitive relationships between mp and emapa
findRelationships()

# write QC
writeQC()

# join the three files to find relationships

# close all output files
closeFiles()

# delete existing relationships
#doDeletes()

# exit(2) if bcp command fails
#doBcp()

sys.exit(0)

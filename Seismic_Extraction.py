#!/usr/bin/env python
# python -m compileall ./  to Compile all code
# >>> import py_compile
# >>> py_compile.compile('Seismic_Extraction.py')
#/OGDR/Seismic_Export_C$ python Seismic_Extraction.pyc
# See the folder size "# du -sh *| sort -h"
#Execution python Seismic_Extraction.pyc
import click
import os
import sys
import pymssql
import argparse
import shutil
from humanize import naturalsize
from shapely.geometry import Point, MultiPoint, Polygon, MultiPolygon
from shapely.wkt import loads
import psycopg2
import psycopg2.extras
from PostgreSQL import PostgreSQL
from time import gmtime, strftime , localtime
import csv
# Addded on 6th March 2018
from shutil import copyfile,rmtree


# sys.path.insert(0, '/home/raj/OGDR/segy')
sys.path.insert(0, '/home/raj/OGDR/Seismic_Export/segy')
sys.path.insert(0, '/root/work/ogdr_utilities/Seismic_Export_C/segy')
import constants_sgy
import LogWriter
import Cls_HeaderReader
import Utilities
import DBUtilities
import segy
import ProcessTif8Files
import TempLateClasses
sys.path.insert(0, '/home/raj/OGDR/Seismic_Export/segd')
sys.path.insert(0, '/root/work/ogdr_utilities/Seismic_Export_C/segd')
from threading import Thread

#import mycode
#jdbc:postgresql://172.16.10.50:5432/
#jdbc:sqlserver://172.16.10.34:1433;

PGCONN = '172.16.10.50'
PGPORT = None
PGUSER = 'postgres'
PGPASS = 'postgres'
PGDB = 'rkms'
TCONN = '172.16.10.34'
#TCONN = '172.16.10.36'
TPORT = None
TUSER = 'ppdmx'
TPASS = 'dataman'
TDB = 'ppdm_ogdr_mirror'
#TDB = 'ppdm_ogdr'
XSDIR = '/var/lib/xstreamline'
MEDIADIR = '/media/'
LIST_BLOCKS = [15, 17, 18, 27, 3, 30, 31, 36, 38, 39, 4, 40, 41, 42, 43, 44, 47, 48, 49, 5, 50, 51, 52, 53, 54, 55,
               56, 57, 58, 59, 6, 60, 61, 62, 62, 64, 65, 66, 67, 7, 8, 9, 70, 71, 72, 73, 74, 75]
PRODUCT_TYPE_2D = ['2D Detail', '2D Swath', '2D']
PRODUCT_TYPE_3D = ['3D Detail', '3D Swath', '3D']

##########  Flag Settings #############################
#Set This flag true for server deployment
SERVER_TEST_FLG = True

# My some feature code @ testing branch
G_NEW_SOMEFEATEUERW- "Rajendra"

# My some feature code @ testing branch next code
G_NEW_SOMEFEATEUERW- "Rajendra"

# My some feature code @ testing branch next code only in master
G_NEW_SOMEFEATEUERW- "Rajendra"

DICT_GEOM_FROM_CSV = False
DICT_PPDM_FROM_CSV = False
DICT_MINMAX_FROM_CSV = False
DICT_GEOM_FROM_CSV_3D = False

#Create CSV files on Server
SERVER_CREATE_CSV_FLG = True    # For using this flag SERVER_TEST_FLG must be True
#SERVER_CREATE_CSV_PATH = '/home/'  # Path to create csv file of database list points which we can to avoid query on databases

##########  Defualt Threshold setting  #############################
DEFAULTPNTRANGETHERSHOLD =3   # Set Threshold for point range
G_OUTPUT_PATH = ''
G_INPUT_PATH = ''
G_SEIS_FORMAT_FILE =''
G_file_segmentlist_within = ''
G_LimitFileSize = 1024000000   # File limit size which are keeping fro direct copy 1Gb

@click.command()
# @click.option('--wkt_Geometry', default='',                 prompt='Enter wkt string of polygon :' ,help='Polygon wkt string .', required=False)
# @click.option('--consession_block_no', default='', prompt='Enter consession block no   :' ,help='Consession Block No.', required=False)
@click.option('--polygon_type', default=1,         prompt='Enter polygon type(By Concesson Block = 1 OR   By wkt = 2):' ,help='Polygon type (1 for Concession Block No / 2 for wkt', required=False)
@click.option('--seis_format_file', default='/root/work/ogdr_utilities/Seismic_Export_C/seis_format.csv',   prompt='Enter seis_format_file with path              :',help='seismic header file information.' )
@click.option('--output_path', default='/data/EXTRACTION/',               prompt='Enter output folder path                      :' ,help='Output folder path.' )
@click.option('--input_path', default='/var/lib/xstreamline/', prompt='Enter Input folder path                       :' , help='Input folder path.' )
@click.option('--media_folder', default='/media/',             prompt='Enter Media folder name                       :' ,help='Media folder name.' )
@click.option('--range_thershold', default=3,                  prompt='Enter Point Thershold Range                   :' ,help='Default Range thershold.' )
@click.option('--filesizelimit', default=1024000000,                  prompt='Enter File size limit                  :' ,help='File size limit, file more than this size need to copy manually by .sh file.' )
#@click.option('--file_segmentlist_within', default="/data/waqar/2020/AOI_SegmentID__WithIn_BLK_LstWKT_new.csv",                  prompt='Enter File name of list Segment id with in Area /Block                   :' ,help='File name of list Segment id with in Area /Block.' )


#def set_parameters( polygon_type, seis_format_file, output_path, input_path, media_folder, range_thershold , file_segmentlist_within):
def set_parameters(polygon_type, seis_format_file, output_path, input_path, media_folder, range_thershold , filesizelimit ):
 for x in range(1):
     click.echo('----------------------------------------------------------------------------------')
     click.echo('------------------------------------- Parameters ---------------------------------')
     if polygon_type == 1 :
        click.echo('polygon_type                   : %s (Concession Block)' % polygon_type)
     elif polygon_type == 2 :
         click.echo('polygon_type                  : %s (WKT geometry)' % polygon_type)
     else:
         return

     click.echo('seis_format_file             : %s' % seis_format_file)
     click.echo('output_path                  : %s' % output_path)
     click.echo('input_path                   : %s' % input_path)
     click.echo('media_folder                 : %s' % media_folder)
     click.echo('range_thershold              : %s' % range_thershold)
     click.echo('filesizelimit              : %s' % filesizelimit)
     #click.echo('file_segmentlist_within      : %s' % file_segmentlist_within)
     click.echo('----------------------------------------------------------------------------------')

     global G_OUTPUT_PATH , G_INPUT_PATH , MEDIADIR ,DEFAULTPNTRANGETHERSHOLD , G_SEIS_FORMAT_FILE , SERVER_CREATE_CSV_PATH , G_LimitFileSize
     G_OUTPUT_PATH = output_path
     G_INPUT_PATH = input_path
     MEDIADIR = media_folder
     DEFAULTPNTRANGETHERSHOLD = range_thershold
     G_SEIS_FORMAT_FILE = seis_format_file
     SERVER_CREATE_CSV_PATH = output_path
     G_LimitFileSize = filesizelimit
     #G_file_segmentlist_within = file_segmentlist_within

     ### Validate variables
     validationErrorFlg = False

     if SERVER_TEST_FLG == True :
        if not os.path.isfile(G_SEIS_FORMAT_FILE):
            print "\nError >> %s file not found." % G_SEIS_FORMAT_FILE
            validationErrorFlg = True

        if not os.path.exists(G_OUTPUT_PATH):
            print "Error >> Output_Path= %s does not exists." % G_OUTPUT_PATH
            validationErrorFlg = True

        if not os.path.exists(G_INPUT_PATH):
            print "Error >>  input_path = %s does not exists." % G_INPUT_PATH
            validationErrorFlg = True

        if int(DEFAULTPNTRANGETHERSHOLD)  < 1 :
            print "Error >> Default Thershold range should be greater then > 1"
            return
        
        if int(G_LimitFileSize)  < 1024000000 :
            print "Error >> File size limit should not be less than 1GB (1024000000 bytes) "
            return

        # # Added on 24 March 2019 ........Raj
        # if len(file_segmentlist_within) > 1 :
        #     #isReadFromFile_WithInFlg = True
        #     #file_segmentlist_within = G_file_segmentlist_within
        #     if not os.path.exists(file_segmentlist_within):
        #         print "Error >> Output_Path= %s does not exists." % G_file_segmentlist_within
        #         return

        if validationErrorFlg == True:
            print "Exiting, Failed in parameter validation ............\n"
            return

     ## If All Validations are succesful Then further code execution
     if polygon_type == 1 or polygon_type == 2 :
         IsDebugTest = False;      # Make This True When you want to overwrite server path paramanters with test paramanetr
         CallMainProgram(polygon_type , IsDebugTest )
     else:
         print("\nError In Input> Invalid Inpuet polygon type,Should be 1 or 2 only !!\n")
         return


def pgsql_connect():
    print("Wait Creating Postgres Connection !!")
    return psycopg2.connect(dbname=PGDB, user=PGUSER, host=PGCONN, password=PGPASS)

def pgsql_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def execute_query(cursor, query):
    return cursor.execute(query)

def pgsql_connect_close(conn):
    return conn.close()

def input_wkt(pPolygon_type, pgsql_cursor):
    """If pPolygon_type == 1, gets the concession number then the wkt through a postgis request and returns the wkt
    Elif polygont_type == 2, tests the wkt and returns it if it's good"""
    concession_nb=None

    if pPolygon_type == 1:
        concession_nb = raw_input("\nType your concession number: ")
        try:
            concession_nb = int(concession_nb)
        except ValueError:
            print("\nInvalid number\n")

        if concession_nb not in LIST_BLOCKS:
            print ("\nInvalid number\n")
            exit(1)

        query = "select ST_AsText(ST_GeometryN(geom, 1)) as wkt from " \
                "concessions where blocknumber='%i';" % concession_nb

        res = execute_query(pgsql_cursor, query)

        for row in pgsql_cursor:
            wkt = row[0]
            print "\nWKT String for concession %i: %s" % (concession_nb, wkt)

    else:
        wkt = raw_input("Enter your WKT string (Polygon type, CRS 4326): \n\n")

        # wkt = "POLYGON((55.8809815640706 18.6210995317394,55.8277131229644 18.6401082017106,55.9235527323046 " \
        #       "18.6551072418571,55.8809815640706 18.6210995317394))"

        try:
            loads(wkt)
        except:
            print "\nBad wkt String\n"
            exit(1)

    return concession_nb, wkt

def CallMainProgram(pPolygon_type , pIsDebugTest ) :

    concession_nb = None
    output_path = G_OUTPUT_PATH
    input_path = G_INPUT_PATH
    seis_format_file   = G_SEIS_FORMAT_FILE
    isSingleSegmentFlg = False
    singleSegmentID = ""
    productType_2Dor3D = ''
    processType_ProcessFieldAll = ''

    isLoadSegmentListFlg = False;
    segmentLstFileName = ""

    isReadFromFile_WithInFlg = False;                # Added on 24 March 2019.
    file_segmentlist_within = G_file_segmentlist_within
    ###############################################################################################

    # -------------------------------------------------------------------------
    # -------------------------------------------------------------------------
    # Testing Parameters Local and Debug Test
    if pIsDebugTest == True :
        output_path = '/home/raj/OGDR/Export_Output2/'                  # Comment these paramenter while server test
        seis_format_file = '/home/raj/OGDR/seis_format.csv';
        SERVER_CREATE_CSV_FLG = True

        pPolygon_type = 2;
        fldrNameBlkNoWKt = '17'
        concession_nb = '17'
        wkt = "POLYGON((57.66977260710 22.83354402560,57.66915403880 21.41878572580,56.09048264260 21.40877609510,56.08341339190 22.83528911580,57.66977260710 22.83354402560))";

        productType_2Dor3D = 'ALL'                #('2D', '3D', 'NONE', 'ALL'):
        processType_ProcessFieldAll = 'ALL'       #('FIELD', 'PROCESS', 'NONE', 'ALL'):

        isSingleSegmentFlg = False                      # Set Flag to RUn On Single Segemnt.
        singleSegmentID = "ELF83TBQ1-Source-ELF83TBQ1-00093E-1"  # Set Single Segment Name Here

        isLoadSegmentListFlg = False;
        segmentLstFileName = '/home/raj/OGDR/19Mar2019/MRG01QNH1.lst'

        # Keep Always false
        isReadFromFile_WithInFlg = False  # Make this flag true when you read  BIN segment with list from File and chnage file name below
        file_segmentlist_within = "/home/raj/OGDR/19Mar2019/AOI_SegmentID_BIN_WithIn_BLK_LstWKT_new.csv";


    # # -------------------------------------------------------------------------
    # # -------------------------------------------------------------------------


    print("\n******************************* ! Seismic Extractions ! ***********************************")

    ###############################################################################################
    print "\n Welcome to Meera Seismic Spatial based Extract Utility\n"

    pgsql_conn = pgsql_connect()
    pgsql_cursor1 = pgsql_cursor(pgsql_conn)

    ###############################################################################################

    # Get Polygon Type or WKT input
    if pIsDebugTest == False:
        if pPolygon_type == 1:
            concession_nb, wkt = input_wkt(pPolygon_type, pgsql_cursor1)
        else:
            wkt = raw_input("Type your WKT String: ")

    ###############################################################################################
    # Parameters single Segmment need to overwrite these here
    isSingleSegmentFlgSet= ""
    if pIsDebugTest == False:
        isSingleSegmentFlgSet = raw_input(
            "\nDo You Want to run only for single segment (eg. \"Yes\" or \"No ):\n ")

        if isSingleSegmentFlgSet.upper() == 'YES' or isSingleSegmentFlgSet.upper() == 'Y':
            ### Select Segment ID  type from User Input
            isSingleSegmentFlg = True
            singleSegmentID = ''
            singleSegmentID = raw_input("\nEnter Segment ID :\n")
            while len(singleSegmentID) < 10:
                print"You have enter wrong Segment ID ...Please Renter again."
                singleSegmentID = raw_input(
                    "Enter Segment ID :\n")
                if singleSegmentID == "none" or singleSegmentID == "" or singleSegmentID.upper() == 'NONE':
                    exit(1)
                elif len(singleSegmentID) > 10:
                    singleSegmentID = singleSegmentID
        else:
            isSingleSegmentFlg = False

    ###############################################################################################
    # Parameters to get Segment list file name
    isLoadSegmentListFlgSet = "";
    if pIsDebugTest == False:
        if isSingleSegmentFlg == False :
            isLoadSegmentListFlgSet = raw_input(
                "\nDo You Want to run with segment list file (eg. \"Yes\" or \"No ):\n ")

            if isLoadSegmentListFlgSet.upper() == 'YES' or isLoadSegmentListFlgSet.upper() == 'Y':
                ### Select Segment ID  type from User Input
                isLoadSegmentListFlg = True

                segmentLstFileName = ''
                segmentLstFileName = raw_input("\nEnter Segment list file name :\n")
                while not os.path.isfile(segmentLstFileName):
                    print"\n File does not exist , Pleae Enter again."
                    segmentLstFileName = raw_input(
                    "Enter Segment list file name :\n")

                    if segmentLstFileName == "none" or segmentLstFileName == "" or segmentLstFileName.upper() == 'NONE':
                        exit(1)
            else:
                isLoadSegmentListFlg = False

    ###############################################################################################
    # Added on 24 March 2019
    # Parameters to get Segment list With in block file name

    #isReadFromFile_WithInFlg = "";
    if pIsDebugTest == False:
        if isSingleSegmentFlg == False:
            if isReadFromFile_WithInFlg == False:
                isReadFromFile_WithInFlgSet = raw_input(
                    "\nDo You Want to load list of segments which are with in inside block (eg. \"Yes\" or \"No ):\n ")

                if isReadFromFile_WithInFlgSet.upper() == 'YES' or isReadFromFile_WithInFlgSet.upper() == 'Y':
                    ### Select Segment ID  type from User Input
                    isReadFromFile_WithInFlg = True

                    file_segmentlist_within = ''
                    file_segmentlist_within = raw_input("\nEnter Segment list file name totally inside(Within) block :\n")
                    while not os.path.isfile(file_segmentlist_within):
                        print"\n File does not exist , Pleae Enter again."
                        file_segmentlist_within = raw_input(
                            "Enter Segment list file name totally inside(Within) block :\n")

                        if file_segmentlist_within == "none" or file_segmentlist_within == "" or file_segmentlist_within.upper() == 'NONE':
                            exit(1)
                else:
                    isReadFromFile_WithInFlg = False


    ###############################################################################################
    # Create main directorry folder

    if pPolygon_type == 1:
        fldrNameBlkNoWKt = Utilities.Create_Dir_StructureMain(str(concession_nb), output_path)
    else:
        fldrNameBlkNoWKt = Utilities.Create_Dir_StructureMain('WKT', output_path)


    ###############################################################################################
    # Create CSV File for Log file Report
    foutLogFileName = output_path + 'Segment_ExtractionsReport_' + fldrNameBlkNoWKt + '.rpt'

    if (foutLogFileName == None):
        print ("Unable to crete xls report file %s", foutLogFileName)
        exit(1)

    clslog_log = LogWriter.clsLogWriter(foutLogFileName)
    clslog_log.writeLine("Meera Seismic Spatial based Extract Utility")
    start_time = strftime("%Y-%m-%d %H:%M:%S", localtime())

    clslog_log.writeLine("\nProcess Start Time :    " + str(start_time) + "\n" )
    print("\nProcess Start Time :    " + str(start_time) + "\n" )
    clslog_log.FlushFile()

    ###############################################################################################
    clslog_log.writeLine("\nCreating SqlServer Connection......................!!")

    # PPDM part Create Sql Connection.....
    mssql_conn = pymssql.connect(server=TCONN, user=TUSER, password=TPASS, database=TDB, login_timeout=20)
    mssql_cursor = mssql_conn.cursor(as_dict=True)

    clslog_log.writeLine("\nSqlServer Connection created !!")

    #################################################################################################
    if pIsDebugTest == False:
        if ( isSingleSegmentFlg == False ) :
            ### Select Product type from User Input
            productType_2Dor3D = raw_input(
                "\nSelect product Type you want to export (eg. \"2D,3D \" or \"all\" or \"none\"):\n")

            while productType_2Dor3D.upper() not in ('2D', '3D', 'NONE', 'ALL'):
                print"You have enter wrong Product Type ...Please Renter again."
                productType_2Dor3D = raw_input(
                    "Select product Type you want to export (eg. \"2D,3D \" or \"all\" or \"none\"):\n")
                if productType_2Dor3D == "none" or productType_2Dor3D == "" or productType_2Dor3D.upper() == 'NONE':
                    exit(1)
                elif productType_2Dor3D.upper() == "2D" or productType_2Dor3D.upper() == "3D" or productType_2Dor3D.upper() == "ALL":
                    productType_2Dor3D = productType_2Dor3D.upper()

            productType_2Dor3D = productType_2Dor3D.upper()

        ##################################################################################################
        ### Select ProcessType  from User Input
        if (isSingleSegmentFlg == False) :
            processType_ProcessFieldAll = raw_input(
                "\nSelect Process Type you want to export (eg. \"Field,Process \" or \"all\" or \"none\"):\n")

            while processType_ProcessFieldAll.upper() not in ('FIELD', 'PROCESS', 'NONE', 'ALL'):
                print"You have enter wrong Process Type ...Please Renter again."
                processType_ProcessFieldAll = raw_input(
                    "Select Process Type you want to export (eg. \"Field,Process \" or \"all\" or \"none\"):\n")
                if processType_ProcessFieldAll == "none" or processType_ProcessFieldAll == "" or processType_ProcessFieldAll.upper() == 'NONE':
                    exit(1)
                elif processType_ProcessFieldAll.upper() == "2D" or processType_ProcessFieldAll.upper() == "3D" or processType_ProcessFieldAll.upper() == "ALL":
                    processType_ProcessFieldAll = processType_ProcessFieldAll.upper()

            processType_ProcessFieldAll = processType_ProcessFieldAll.upper()

        ##################################################################################################

    clslog_log.writeLine("\nSelected Parameters...............................................")

    clslog_log.writeLine("\n\nPolygon Type :" + str(pPolygon_type) )
    if pPolygon_type == 1 :
        clslog_log.writeLine("\nConcession Block :" + str(concession_nb))
    else:
        clslog_log.writeLine("\nPolygon Type :" + str(wkt))

    clslog_log.writeLine("\nProcess Type :" + processType_ProcessFieldAll )
    clslog_log.writeLine("\nProduct Type :" + productType_2Dor3D)
    clslog_log.writeLine("\nisLoadSegmentListFlg = " + str(isLoadSegmentListFlg)  + ";  Segement list name :" + segmentLstFileName)
    clslog_log.writeLine("\nisSingleSegmentFlg = " + str(isSingleSegmentFlg) + ";  Segement name :" + singleSegmentID)
    clslog_log.FlushFile()


    ##################################################################################################
    ## Read CSV file segments Header Format
    print("\nReading segments Header Format...................................")
    
    clslog_log.writeLine("\nReading segments Header Format ..................................")
    y = Cls_HeaderReader.cls_SegYCSVFormat()

    if os.path.isfile(seis_format_file):
        dict_segyFormat = y.fromCsvFile(seis_format_file)
    else:
        print seis_format_file + " seis_format.csv reading error or File not found."
        clslog_log.writeLine("\nError: " + seis_format_file + " seis_format.csv reading error or File not found.")
        clslog_log.closefile();
        exit(1)

    ## Sies format input file missing ...
    if dict_segyFormat == False:
        print "seis_format.csv reading error or File not found."
        clslog_log.closefile();
        exit();

    clslog_log.writeLine("\nReading segments Header Format Completed ...................")
    print("\nReading segments Header Format Completed ...........................................")
    clslog_log.FlushFile()
    ##################################################################################################
    print("\nReading segments from postgis Other than BIN ...................")
    segment_id_set = set()

    segment_id_otr_BIN_WithIn_set = set()   #Set of segment id other than bin which is totally inside area
    segment_id_BIN_WithIn_set = set()    # Set of BIN segements with in block
    segment_id_WithIn_set = set()  # Set of segements with in block

    segment_id_set_bin_overlap = set()

    dict_withInShape = {}       # Dictinary for point with in Area
    dict_geom = {}
    # Get lines segment id for overlaping block other than BIN

    #if isSingleSegmentFlg == False :
    if isSingleSegmentFlg == False:
        clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
        print("\n-----------------------------------------------------------------------------------------------------------------")
        clslog_log.writeLine("\nReading segments from postgis Other than BIN ...................")
        clslog_log.FlushFile()
        print("\nReading segments from postgis Other than BIN ...................")
        SERVER_CREATE_CSV_FLG = True

        # Commented for BIN Data
        if productType_2Dor3D == 'ALL' or productType_2Dor3D == '2D' or productType_2Dor3D == '3D':
            segment_id_set , dict_geom  = DBUtilities.N_Get_Dict_DictGeom_and_DictGeomNew(pWkt=wkt, pPgsqlConn=pgsql_conn,
                                                                                     pDICT_GEOM_FROM_CSV_Flg=isLoadSegmentListFlg,
                                                                                     pDICT_GEOM_FROM_CSV=segmentLstFileName,
                                                                                     pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                                                     pBlockNumber=concession_nb,
                                                                                     pPolygonType=pPolygon_type,
                                                                                     pProcessTypeFlg=processType_ProcessFieldAll,
                                                                                     pProductTypeFlg=productType_2Dor3D,
                                                                                     pSERVER_CREATE_CSV_PATH = output_path)

        clslog_log.writeLine("\nSegments from postgis Other than BIN :" + str(len(segment_id_set)))
        clslog_log.writeLine("\nReading segments from postgis Other than BIN ...................Completed.")
        clslog_log.FlushFile()
        print("\nSegments from postgis Other than BIN :" + str(len(segment_id_set)))
        print("\nReading segments from postgis Other than BIN ...................Completed.")
        print("\n-----------------------------------------------------------------------------------------------------------------")

        if productType_2Dor3D == 'ALL' or productType_2Dor3D == '3D':
            print(
                "\n-----------------------------------------------------------------------------------------------------------------")
            clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
            clslog_log.writeLine("\nReading BIN segments totally inside (With in) In Polygon area...................")
            clslog_log.FlushFile()
            print("\nReading BIN segments totally inside (With in) In Polygon area...................")
            if isReadFromFile_WithInFlg == False:
                lst_3DBINWithInSegement, dict_withInShape = DBUtilities.N_Find3dLinesWithInShape(pPgSqlConn=pgsql_conn,
                                                                                                 pWktPolyGon=wkt,
                                                                                                 pBlockNumber=concession_nb,
                                                                                                 pPolygonType=pPolygon_type,
                                                                                                 pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                                                                 pSERVER_CREATE_CSV_PATH=output_path,
                                                                                                 pProcessTypeFlg=processType_ProcessFieldAll,
                                                                                                 pProductTypeFlg=productType_2Dor3D,
                                                                                                 pSrid='4326')

                if len(lst_3DBINWithInSegement) > 0:
                    for mm in range(len(lst_3DBINWithInSegement)):
                        segment_id_BIN_WithIn_set.add(lst_3DBINWithInSegement[mm])
            else:
                # Read segment id from CSV file.
                with open(file_segmentlist_within, 'r') as f:
                    segment_id_BIN_WithIn_setAll = set()
                    reader = csv.DictReader(f)
                    row_count = 0
                    for row in reader:
                        if '-BIN-' in str(row['segment_id']):
                            segment_id_BIN_WithIn_setAll.add(row['segment_id'])
                        else:
                            segment_id_otr_BIN_WithIn_set.add(row['segment_id'])

                    del reader

                    # Get Segment id which with in set as well as in list
                    segment_id_BIN_WithIn_set = segment_id_set.intersection(segment_id_BIN_WithIn_setAll);

                    segment_id_set = segment_id_set - segment_id_BIN_WithIn_set;
                    segment_id_set = segment_id_set - segment_id_otr_BIN_WithIn_set;


        clslog_log.writeLine("\nBIN Segments totally inside(With In)  :     " + str(len(segment_id_BIN_WithIn_set)))
        clslog_log.writeLine("\nReading BIN segments totally inside (With in) Polygon area...............Completed.")
        clslog_log.FlushFile()
        print("\nBIN Segments totally inside(With In) :     " + str(len(segment_id_BIN_WithIn_set)))
        print("\nReading BIN segments totally inside (With in) Polygon area...............Completed.")
        print("\n-----------------------------------------------------------------------------------------------------------------")
        #--------------------------------------------------------------------------------------------------------------
        clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
        #Get BIN lines segment id
        lst_3DBINIntersectSegement = list()
        if isLoadSegmentListFlg == False:               # Not required when if isLoadSegmentListFlg True bcas we rgetting list directly
            if ( productType_2Dor3D == 'ALL' or productType_2Dor3D == '3D'):
                if processType_ProcessFieldAll == 'ALL' or processType_ProcessFieldAll == 'PROCESS':
                    print("\n-----------------------------------------------------------------------------------------------------------------")
                    clslog_log.writeLine("\nReading Overlap segments from postgis for BIN data...................")
                    clslog_log.FlushFile()
                    print("\nReading Overlap segments from postgis for BIN data...................")
                    lst_3DBINIntersectSegement , segment_id_set_bin_overlap = DBUtilities.N_Find3dLinesIntersectWithShape(pPgSqlConn=pgsql_conn, pWktPolyGon=wkt,
                                                                                        pBlockNumber=concession_nb,
                                                                                        pPolygonType=pPolygon_type,
                                                                                        pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                                                        pSERVER_CREATE_CSV_PATH=output_path,
                                                                                        pSrid='4326')


                    # if len(lst_3DBINIntersectSegement) > 0:
                    #     for mm in range(len(lst_3DBINIntersectSegement)):
                    #         segment_id_set_bin_overlap.add(lst_3DBINIntersectSegement[mm])

                    clslog_log.writeLine("\nBIN Segments overlap on area:   " + str(len(segment_id_set_bin_overlap)))
                    clslog_log.writeLine("\nReading Overlap segments from postgis for BIN data...............Completed.")
                    clslog_log.FlushFile()
                    print("\nBIN Segments overlap on area:  " + str(len(segment_id_set_bin_overlap)))
                    print("\nReading Overlap segments from postgis for BIN data...............Completed.")
                    
                    if len(segment_id_set_bin_overlap) > 0 :
                        segment_id_set_bin_overlap = segment_id_set_bin_overlap - segment_id_BIN_WithIn_set

        # --------------------------------------------------------------------------------------------------------------
    else :
        #segment_id_set.add('JPX82REG1-CMP-JPX82REG1-01002-1')
        segment_id_set.add(singleSegmentID)

    # Added on 10th April , to get point info of BIn overlap segment
    segment_id_setTemp = set()
    if len(segment_id_set_bin_overlap) > 0:
        segment_id_setTemp = segment_id_set.union(segment_id_set_bin_overlap);

    ###############################################################################
    print("\n-----------------------------------------------------------------------------------------------------------------")
    clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
    print("\nReading segments start and end point from postgis ..........")
    clslog_log.writeLine("\nReading segments start and end point from postgis ..........")
    clslog_log.FlushFile()
    # Get Min Max Point from GIS database for 3D point Utilisation
    # for seg in segment_id_set:
    #     segment_id_set_string += "\'%s\'," % seg
    n_dict_gis_st_end = {}
    # Added path , block number, server path variable pSERVER_CREATE_CSV_PATH on 18 Nov 2018
    # dict_gis_st_end= DBUtilities.Get_Dict_DictGisStEnd( pPgsqlConn=pgsql_conn, pSegment_id_set= segment_id_set,  pDICT_MINMAX_FROM_CSV= DICT_MINMAX_FROM_CSV, pSERVER_TEST_FLG = SERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG = SERVER_CREATE_CSV_FLG )
    n_dict_gis_st_end = DBUtilities.N_Get_Dict_DictGisStEnd(pPgsqlConn=pgsql_conn, pSegment_id_set=segment_id_setTemp,
                                                        pDICT_MINMAX_FROM_CSV=DICT_MINMAX_FROM_CSV,
                                                        pSERVER_TEST_FLG=SERVER_TEST_FLG,
                                                        pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                        pSERVER_CREATE_CSV_PATH=output_path,
                                                        pPolygonType=pPolygon_type,
                                                        pBlockNumber=concession_nb)

    clslog_log.writeLine("\nSegments start and end point Records found from postgis: " + str(len(n_dict_gis_st_end)))
    segment_id_setTemp = None

    clslog_log.writeLine("\nReading segments start and end point from postgis ...........Completed.")
    print("\nReading segments start and end point from postgis ............Completed.")
    
    clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
    print("\n-----------------------------------------------------------------------------------------------------------------")
    
    # --------------------------------------------------------------------------------------------------------------#
    #Process to get segement other than BIN who totally Inside in Area
    clslog_log.writeLine("\nProcessing segments other than BIN ............................")
    clslog_log.writeLine("\nChecking segment totally inside (with in) in Area..............")
    print("\nProcessing segments other than BIN ............................")
    print("\nChecking segment totally inside (with in) in Area..............")
    
    clslog_log.FlushFile()
    # Added on 20 March 2019 ..........To create segement list other than BIN
    if pPolygon_type == 1:
        fsv1 = open(output_path + 'SegmentID_OtherThanBIN_WithIn_BLK_Lst' + str(concession_nb) + '.csv', 'w+')
    else:
        fsv1 = open(output_path + 'SegmentID_OtherThanBIN_WithIn_BLK_Lst' + 'WKT' + '.csv', 'w+')

    fsv1.write('segment_id,min_pnt,max_pnt,TotalPnt')

    for dd in dict_geom.itervalues():
        segid2 = dd["segment_id"]

        #print segid2
        if n_dict_gis_st_end.has_key(segid2):
            varGisStEndPnt1 = n_dict_gis_st_end[segid2]
            # vSegSpStart1 = varGisStEndPnt1['g_seg_stPnt']  # postgis data
            # vSegSpEnd1 = varGisStEndPnt1['g_seg_endPnt']  # postgis data
            vTotalPnt =  varGisStEndPnt1['Total_pnt']
            
            if dd["pnt_diff"] == vTotalPnt:
                segment_id_otr_BIN_WithIn_set.add(segid2)
                clslog_log.writeLine("\n    "+ segid2 )
                fsv1.write('\n' + str(segid2) + ',' + str(varGisStEndPnt1['g_seg_stPnt']) + ',' + str(varGisStEndPnt1['g_seg_endPnt']) + ',' + str(vTotalPnt) )


    fsv1.close()

    #Set creted to list segments other than bin overlap with area.
    segment_id_set_otherThanBinOverlap = set()

    if len(segment_id_otr_BIN_WithIn_set) > 0 :
        segment_id_set = segment_id_set - segment_id_otr_BIN_WithIn_set
        segment_id_set_otherThanBinOverlap = segment_id_set - segment_id_otr_BIN_WithIn_set

    # Added on 20 March 2019 ..........To create segement list other than BIN...............End

    clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
    print("\n-----------------------------------------------------------------------------------------------------------------")

    clslog_log.writeLine("\nSummary of Segements : -----------------------------------------------")
    print("\nSummary of Segements : -----------------------------------------------")

    
    clslog_log.writeLine("\n    Other than BIN Segments Totally Inside : " + str( len(segment_id_otr_BIN_WithIn_set)) )
    print("\n   Other than BIN Segments Totally Inside : " + str( len(segment_id_otr_BIN_WithIn_set)) )

    clslog_log.writeLine("\n    Other than BIN Segements Overlap : " + str( len(segment_id_set_otherThanBinOverlap)) )
    print("\n   Other than BIN Segements Overlap : : " + str( len(segment_id_set_otherThanBinOverlap)) )

    clslog_log.writeLine("\n\n    BIN Segments Totally Inside : " + str( len(segment_id_BIN_WithIn_set)) )
    print("\n\n   BIN Segments Totally Inside : " + str( len(segment_id_BIN_WithIn_set)) )
    
    clslog_log.writeLine("\n    BIN Segements Overlap : " + str( len(segment_id_set_bin_overlap)) )
    print("\n   BIN Segements Overlap : : " + str( len(segment_id_set_bin_overlap)) )
    
    clslog_log.FlushFile()

    segment_id_WithIn_set = segment_id_otr_BIN_WithIn_set.union(segment_id_BIN_WithIn_set)

    TotalSegmentCnt = 0
    if len(segment_id_set_bin_overlap) > 0:
        segment_id_set = segment_id_set.union(segment_id_set_bin_overlap);
        

    if len(segment_id_set) > 0 or len(segment_id_WithIn_set) > 0 :
        TotalSegmentCnt = len(segment_id_set) + len(segment_id_WithIn_set)
        
    
    clslog_log.writeLine("\n\n    Segments Totally inside (With In) in Area : " + str(len(segment_id_WithIn_set)))
    print("\n\n   Segemnt Totally inside (With In) in Area : " + str(len(segment_id_WithIn_set)))

    clslog_log.writeLine("\n    Segments Overlap  with Area : " + str(len(segment_id_set)))
    print("\n   Segments Overlap  with Area : : " + str(len(segment_id_set)))

    clslog_log.writeLine("\n   ------------------------------------------------------------------")
    print("\n    ------------------------------------------------------------------")

    clslog_log.writeLine("\n     Total Segements found : " + str(TotalSegmentCnt))
    print ("\n  Total Segements found : " + str(TotalSegmentCnt))

    clslog_log.writeLine("\n-----------------------------------------------------------------------------------------------------------------")
    print("\n-----------------------------------------------------------------------------------------------------------------")

    clslog_log.writeLine("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    TotalSegmentWithINCnt =0 ;
    dict_ppdm ={}
    
    print("+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    print("Copying files for with in segments begin...........................")
    clslog_log.writeLine("\nCopying files for with in segments begin...........................")
     

    #Flag For Testing purpose
    #tstFlg= False
    copyFileCnt = 0;
    copyFileCnt4WithIn = 0;

    if productType_2Dor3D == '3D' or productType_2Dor3D == 'ALL':
        #if len(segment_id_WithIn_set) > 0 and tstFlg == True:   #Flag For Testing purpose
        if len(segment_id_WithIn_set) > 0 :
            segment_id_WithIn_setFoundRec = set();

            TotalSegmentWithINCnt = len(segment_id_WithIn_set)

            clslog_log.writeLine("\nProcessing Segments totally inside(with in) in the polygon......")
            clslog_log.writeLine("\nSegements Counts :    " + str(TotalSegmentWithINCnt));

            # Change this code  on 7 th april get direct record from new table
            # dict_ppdm = DBUtilities.Get_Dict_Dict_ppdm(pMssql_conn=mssql_conn, pSegment_id_set=segment_id_WithIn_set,
            #                                            pDICT_PPDM_FROM_CSV=False,
            #                                            pSERVER_TEST_FLG=True,
            #                                            pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
            #                                            pSERVER_CREATE_CSV_PATH=output_path,
            #                                            pPolygonType=pPolygon_type,
            #                                            pBlockNumber=concession_nb
            #                                            )
            # Added on 7 apr 2019......>>Rajendra
            dict_ppdm = DBUtilities.Get_Dict_Dict_ppdm_From_T_SeisEx(pMssql_conn=mssql_conn, pSegment_id_set=segment_id_WithIn_set,
                                                      pDICT_PPDM_FROM_CSV=False,
                                                      pSERVER_TEST_FLG=True,
                                                      pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                      pSERVER_CREATE_CSV_PATH=output_path,
                                                      pPolygonType=pPolygon_type,
                                                      pBlockNumber=concession_nb
                                                      )

            TotaDictPpdmWithInCnt = len(dict_ppdm)
            segmentWithInCntNo = 0;

            clslog_log.writeLine("\nTotal file records found Counts :    " + str(TotaDictPpdmWithInCnt));
            print("\nTotal file records found Counts :    " + str(TotaDictPpdmWithInCnt));

             # Create CSV File for Log file Report
            foutCopyBatFileName = output_path + 'Copy_File' + fldrNameBlkNoWKt + '.bat'

            if (foutCopyBatFileName == None):
                print ("Unable to create report file %s", foutCopyBatFileName)
                exit(1)

            clslog_cpBat = LogWriter.clsLogWriter(foutCopyBatFileName)
            clslog_cpBat.writeLine("\nProcess copying files begin :.........................." )
            
            
            for dd in dict_ppdm.itervalues():
                seg_id = dd['geom_segment_id']
                copyFilesFlg = True
                # if not seg_id in ('BIN') :
                #     copyFilesFlg = False
                #     # Take max min gis point in from database
                #     if n_dict_gis_st_end.has_key(seg_id):
                #         varGisStEndPnt1 = n_dict_gis_st_end[seg_id]
                #         vSegSpStart1 = varGisStEndPnt1['g_seg_stPnt']  # postgis data
                #         vSegSpEnd1 = varGisStEndPnt1['g_seg_endPnt']  # postgis data
                #
                #         #Take max min gis point in area
                #         if dict_withInShape.has_key(seg_id):
                #             varMinMaxUInShape  = dict_withInShape[seg_id]
                #             vSegSpStartInShp = varMinMaxUInShape['max_pnt']
                #             vSegSpEndInShp = varMinMaxUInShape['min_pnt']
                #
                #             if vSegSpStartInShp == vSegSpStart1 and vSegSpEndInShp == vSegSpEnd1:
                #                 copyFilesFlg = True

                if  copyFilesFlg:
                    segment_id_WithIn_setFoundRec.add(seg_id)

                    segmentWithInCntNo = segmentWithInCntNo + 1;

                    clslog_log.writeLine("\n    Processing Segment :" + seg_id + ';   File no.: ' + str(segmentWithInCntNo) + " of " + str(TotaDictPpdmWithInCnt))
                    print("\n   Processing Segment :" + seg_id + ';   File No.: ' + str(segmentWithInCntNo) + " of " + str(TotaDictPpdmWithInCnt))
                    clslog_log.FlushFile()

                    vOrgnlFileName = dd["original_file_name"]
                    vProcName4Fldr = dd["processing_name"]
                    vProcSetType4SubFldr = dd["proc_set_type"]
                    vSectName4SubFldr = dd['section_name']
                    vStorId = dd['store_id']
                    vLocReference = dd['location_reference']
                    vPointType = dd['point_type']
                    vProdTyp = dd['product_type']

                    # Define 2d or 3d type folder
                    vType2dOr3d = Utilities.N_GetType2dOr3d(vProcSetType4SubFldr, vProdTyp)
                    #print vType2dOr3d;

                    if 'Error' in vType2dOr3d:
                        # d.error = str(d.error) + 'Product Type not like 2D or 3D ;'
                        print 'Product Type not like 2D or 3D ;'
                        clslog_log.writeLine('\n         Product Type not like 2D or 3D ;')
                        continue

                    input_path = XSDIR + '/' + vStorId + '/' + MEDIADIR + vLocReference + '/'
                    srcFileName = input_path + vOrgnlFileName

                    if not os.path.isfile(srcFileName):
                        # Check filepath in remarks column
                        # clslog_log.writeLine('\n>>' + srcFileName + ' is not found so set from remark  as ' + v["remark"])
                        print('   >>' + srcFileName + ' is not found so set from remark  as ' + dd["remark"])
                        srcFileName = dd["remark"]

                    if not os.path.isfile(srcFileName):  # Local Path For Loacal Tsting Purpose
                        srcFileName = "/home/raj/OGDR/Test_Sqy/" + vOrgnlFileName

                    if not os.path.isfile(srcFileName):
                        print srcFileName + ' Segy File not Found ;'
                        # d.error = str(d.error) + srcFileName + ' Segy File not Found ;'
                        # clslog.WriteCsvLineUsingCls(clsSegDet=d)
                        continue
                    else:
                        # ------------------------------------------------------------------------------
                        try:
                            dPath = Utilities.Create_Dir_Structure(fldrNameBlkNoWKt, vProcName4Fldr,
                                                                   vProcSetType4SubFldr,
                                                                   vSectName4SubFldr, output_path, vType2dOr3d)
                        except:
                            print("\n   Error : Unable to create Folder structure.")
                            clslog_log.writeLine("\n        Error : Unable to create Folder structure.")
                            # d.error = str(d.error) + 'Unable to create Folder structure for .' + fldrNameBlkNoWKt + '|' + vProcName4Fldr + '|' + vProcSetType4SubFldr + '|' + vSectName4SubFldr + '|' + output_path + '|' + vType2dOr3d
                            clslog_log.writeLine(
                                '\n        Unable to create Folder structure for .' + fldrNameBlkNoWKt + '|' + vProcName4Fldr + '|' + vProcSetType4SubFldr + '|' + vSectName4SubFldr + '|' + output_path + '|' + vType2dOr3d)
                            continue

                        # Set file name as per Min Max Point
                        dstFileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy', '').replace('.segy',
                                                                                                    '') + '_' + str(
                            dd["min_ffid_start"]) + '_' + str(dd["max_ffid_end"]) + '_' + vPointType + '.sgy'

                        statinfo = os.path.getsize(srcFileName)

                        print("    File size : " + str("{:,}".format(statinfo)) + " bytes"  )
                        #Write down file name copyfile batch fiel so user will copy this by another way, to speed up the process
                        #if (statinfo > 1536000000):  # More than 1. gb
                        if (statinfo > G_LimitFileSize):
                            copyFileCnt = copyFileCnt + 1 ;
                            #Thread(target=shutil.copyfile, args=[srcFileName, dstFileName]).start()
                            #clslog_log.writeLine("\n    File Copy at  : " + dstFileName)

                            clslog_cpBat.writeLine("\n echo \"copying file : " + str(copyFileCnt)  )
                            clslog_cpBat.writeLine("\n echo \"File size : " + str("{:,}".format(statinfo)) + " bytes"  )
                            clslog_cpBat.writeLine("\n echo \"copying file " + srcFileName + " to " + dstFileName + " ..............\"" )
                            clslog_cpBat.writeLine("\n cp \"" + srcFileName + "\" \"" + dstFileName + "\"" )
                            clslog_cpBat.FlushFile()
                        else:
                            try:
                                copyFileCnt4WithIn = copyFileCnt4WithIn + 1;
                                copyfile(srcFileName, dstFileName)
                                clslog_log.writeLine("\n        File Copy at  : " + dstFileName)
                            except:
                                print("     Error : File copy error:" + srcFileName + " to " + dstFileName)   
                                clslog_log.writeLine("\n       Error : File copy error:" + srcFileName + " to  " + dstFileName)

                        
                        clslog_log.writeLine("\n    ---------------------------------------------------------------------------------------------------------------")

            clslog_cpBat.writeLine("\nProcess copying files finished :.........................." )
            clslog_cpBat.closefile();

            clslog_log.writeLine("\n    No Seis file records founds for following segements ids:")
            setR = segment_id_WithIn_set - segment_id_WithIn_setFoundRec

            for segId2 in setR:
                clslog_log.writeLine("\n        " + segId2)

            clslog_log.writeLine("\n    ---------------------------------------------------------------------------------------------------------------")
            clslog_log.writeLine("\nTotal segements(with in ) in polygon processed  :   " + str ( TotalSegmentWithINCnt) )
            clslog_log.writeLine("\nTotal segements count having file records found :   " + str ( len(setR)) )
            clslog_log.writeLine("\nTotal File copied for with in segments :   " + str ( copyFileCnt4WithIn + copyFileCnt) ) 

            clslog_log.writeLine("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    ## FProcessing of single segement ..............................

    set_TotalSegOvlpFndInSeisFile = set();
    TotalSegOvlpFndInSeisFileCnt = 0 ;
    TotalFileCnt = 0;
    set_TotalSeisFileDataNotFnd = set();
    TotalSeisFileDataNotFndCnt = 0 ;

    set_TotalNoGisIntersectionNotFndCnt = set();
    TotalNoGisIntersectionNotFndCnt=0;
    set_TotalNoOvrlpWithSiesFileData = set();
    TotalNoOvrlpWithSiesFileDataCnt =0 ;
    pointFreq = 1 ;

    segmentCntNo = TotalSegmentWithINCnt + 1;

    segment_id_set = segment_id_set - segment_id_WithIn_set

    segmentCntNo1 =  1
    TotalSegmentCntFromSet  =  len(segment_id_set)

    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0
    segment_id_set_subset = set()

    totalquercnt= len(segment_id_set)/1000
    if (len(segment_id_set) % 1000 ) > 0 :
        totalquercnt = totalquercnt + 1

    # Processing Segements one by one
    for segId1 in segment_id_set:
        # Added on 2 apr 2019......>>Rajendra
        seg_nb += 1
        seg_query += 1
        segment_id_set_string1 += "\'%s\'," % segId1
        segment_id_set_subset.add(segId1)

        if seg_nb % 1000 == 0 or seg_nb == len(segment_id_set):
            segment_id_set_string1 = segment_id_set_string1[:-1]
            query_count += 1

            print("\n+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
            print("\nProcessing query Count :  " +  str(query_count)  + " /  " + str (totalquercnt) )

            dict_pnt_freq = {}
            dict_pnt_freq = DBUtilities.N_GetPointFreqFromPostgis_Dict(pgsql_conn , segment_id_set_string1)

            dict_ppdm_seisFileRng_All={}
            dict_ppdm_seisFileRng_All= DBUtilities.N_Get_PppdmSeisFileRange_Dict( segment_id_set_string1, mssql_conn )

            segment_id_set_string1 = "";

            for segId in segment_id_set_subset:
                print("\n---------------------------------------------------------------------------------------------------------------")
                clslog_log.writeLine("\n---------------------------------------------------------------------------------------------------------------")
                segmentCntNo1  =   segmentCntNo1 + 1;
                print("\nProcessing Segment :" + segId + '   Segment No.: ' + str (segmentCntNo1) +" of " + str(TotalSegmentCntFromSet))
                clslog_log.writeLine("\nProcessing Segment :" + segId + '   Segment No.: ' + str(segmentCntNo1) + " of " + str(TotalSegmentCntFromSet))
                clslog_log.FlushFile()
                segId = str(segId);
                dict_ResultOvlpRngs = {}

                isBinFlg = False
                if '-BIN-' not in segId:
                        clslog_log.writeLine("\n    Reading Point Ranges from Post Gis :    " + segId)
                        
                        #poitnFreq = DBUtilities.N_GetPointFreqFromPostgis(pgsql_conn, segId)
                        if segId in dict_pnt_freq.keys():
                            poitnFreq = dict_pnt_freq[segId]["freq"]
                        else:
                            clslog_log.writeLine("\n    Point frequency not found for segment ...." + str(segId))
                            poitnFreq = 1;

                        clslog_log.writeLine("\n    Reading Point freq from Post Gis      :    " + str(poitnFreq) )
                        listPntRangeInside = DBUtilities.N_GetGisPointRangesInsideBlock(pPolygon_type ,  pgsql_conn, poitnFreq, segId , concession_nb , wkt )

                        isBinFlg = False
                        if (poitnFreq < 0):
                            print("\n   PLz update point frequency in seis_segment_pointfreq table for segment: " + segId)
                            continue
                else:
                    #print("(BIN) 3D POINT Data")
                    isBinFlg = True
                    poitnFreq = 1

                    if segId in n_dict_gis_st_end.keys():
                        varGisStEndPnt = n_dict_gis_st_end[segId]
                        vSegSpStart = varGisStEndPnt['g_seg_stPnt']  # postgis data
                        vSegSpEnd = varGisStEndPnt['g_seg_endPnt']  # postgis data
                    else:
                        #d.error = str(d.error) + 'Start End Point Missing in PostGIS Datatabase.;'
                        print 'Start End Point Missing in PostGIS Datatabase.;'
                        clslog_log.writeLine("\n    Start End Point Missing in PostGIS Datatabase :" + segId)
                        continue

                    # Spatial Query to get point inside block or WKT
                    clslog_log.writeLine("\n    Reading Point Ranges from Post Gis :    " + segId)
                    listPntRangeInsideN , listPntRangeInside  = DBUtilities.Get_ListOfPointRangeInsideInPolygonUsingSegID(
                    pPgSqlConn=pgsql_conn,
                    pSegID=segId,
                    pWktPolyGon=wkt,
                    pSegStPntNo=vSegSpStart,
                    pSegEndPntNo=vSegSpEnd,
                    pBlockNumber=concession_nb,
                    pPolygonType=pPolygon_type,
                    pSrid='4326')

                if len(listPntRangeInside) == 0 :
                    clslog_log.writeLine("\n    " + segId + " : PostGIS Point intersections not found.........."  )
                    print ("\n    " + segId + " : PostGIS Point intersections not found.........."  )
                    TotalNoGisIntersectionNotFndCnt = TotalNoGisIntersectionNotFndCnt + 1;
                    continue

                # Get Seis File Details and respective ranges
                clslog_log.writeLine("\n    Reading Seis File Ranges           :  " + segId)
                dict_ppdm_seisFileRng = {}

                #Taking so much time
                #dict_ppdm_seisFileRng = DBUtilities.N_Get_PppdmSeisFileRange(segId, mssql_conn , isBinFlg )
                if segId in dict_ppdm_seisFileRng_All.keys():
                    dict_ppdm_seisFileRng = dict_ppdm_seisFileRng_All[segId];

                segDFileList = list();

                spFoundFlg = False  # Flag to set sp point matching found check , if this is false then we can take ffid overlap set oor source type segments
                if len(dict_ppdm_seisFileRng) > 0 :
                    #Loop for Gis points Ranges
                    clslog_log.writeLine("\n    Processing PostGIS Point ranges Begin : " )
                    if len(listPntRangeInside) > 0 :
                        for clsr in listPntRangeInside:

                            #clsd = TempLateClasses.clsRngDetais(clsr)
                            print clsr.segId

                            gisRngMinPnt = int(clsr.stPnt)
                            gisRngMaxPnt = int(clsr.endPnt)

                            clslog_log.writeLine("\n    ---------------------------------------------------------------------------------------------------------------")
                            clslog_log.writeLine("\n    Processing Gis Point Range :" + str(gisRngMinPnt) + ' --- ' + str(gisRngMaxPnt) )

                            # Loop for Seis file point Ranges
                            for eleFileRng in dict_ppdm_seisFileRng.itervalues():
                                seisFileName = eleFileRng['original_file_name']
                                #print seisFileName ,' : ',  eleFileRng["proc_set_type"]
                                clslog_log.writeLine("\n    " + seisFileName )

                                # Check file is not type od SEGD
                                if eleFileRng["proc_set_type"] == 'FIELD SEGD':
                                    if not seisFileName in segDFileList:
                                        segDFileList.append(seisFileName)
                                        #print (seisFileName + " is a SEGD file.")
                                        clslog_log.writeLine("\n    " + seisFileName + "is a SEGD file.")
                                    continue

                                #Check file is not type od SEGD
                                if eleFileRng["proc_set_type"] != 'FIELD SEGD':
                                    isReverseFlgFFID = False;
                                    isReverseFlgSP = False;
                                    isSP_Flg = False;

                                    if ( "-BIN-" in segId.upper() or "-OTHER-" in segId.upper() or "-CMP-" in segId.upper() ):
                                        seisFileRngMinPnt = int(eleFileRng['ffid_start'])
                                        seisFileRngMaxPnt = int(eleFileRng['ffid_end'])

                                        if seisFileRngMaxPnt < seisFileRngMinPnt:
                                            seisFileRngMaxPnt  = int(eleFileRng['ffid_start'])
                                            seisFileRngMinPnt = int(eleFileRng['ffid_end'])
                                            isReverseFlgFFID = True

                                    else:  # For SP points
                                        # For This type get both ranges of SP aa well as FFID
                                        isSP_Flg = True;
                                        seisFileRngMinPntSP = eleFileRng['sp_start']
                                        seisFileRngMaxPntSP = eleFileRng['sp_end']

                                        if seisFileRngMaxPntSP < seisFileRngMinPntSP:
                                            seisFileRngMaxPntSP = eleFileRng['sp_start']
                                            seisFileRngMinPntSP = eleFileRng['sp_end']
                                            isReverseFlgSP = True

                                        seisFileRngMinPnt = eleFileRng['ffid_start']
                                        seisFileRngMaxPnt = eleFileRng['ffid_end']

                                        if seisFileRngMaxPnt < seisFileRngMinPnt:
                                            seisFileRngMaxPnt = eleFileRng['ffid_start']
                                            seisFileRngMinPnt = eleFileRng['ffid_end']
                                            isReverseFlgFFID = True

                                    setGisPnts = set(range(gisRngMinPnt, gisRngMaxPnt + 1, 1))

                                    # Get SP point overlap Range
                                    setOvrlpSP_FFID = set();
                                    setOvrlpSP = set();
                                    if isSP_Flg == True:
                                        setResultSP = set();
                                        setSeisFileSP = set(range(seisFileRngMinPntSP, seisFileRngMaxPntSP + 1, 1))
                                        setOvrlpSP = setGisPnts.intersection(setSeisFileSP)

                                        if len(setOvrlpSP) > 0:
                                            overlapRngMinSP = int(min(setOvrlpSP))
                                            overlapRngMaxSP = int(max(setOvrlpSP))

                                            spFoundFlg = True;  # Flag to set sp point matching found check

                                            #Get ffid of respectiove SP points
                                            overlapRngMinSP_ffid, overlapRngMaxSP_ffid = Utilities.N_GetFFidFromSpPoints(seisFileRngMinPnt , seisFileRngMaxPnt , overlapRngMinSP, overlapRngMaxSP, seisFileRngMinPntSP, seisFileRngMaxPntSP , isReverseFlgSP , isReverseFlgFFID  )
                                            if overlapRngMinSP_ffid > 0 and overlapRngMaxSP_ffid > 0 :
                                                setOvrlpSP_FFID = set(range(overlapRngMinSP_ffid, overlapRngMaxSP_ffid + 1))

                                    # Get FFID point overlap Range
                                    setSeisFileFFID = set(range(seisFileRngMinPnt, seisFileRngMaxPnt + 1, 1))
                                    setResultFFID = setGisPnts.intersection(setSeisFileFFID)
                                    setOvrlpFFID = set();
                                    if len(setResultFFID) > 0:
                                        print min(setResultFFID)
                                        print max(setResultFFID)
                                        print setResultFFID

                                        overlapRngMin = int(min(setResultFFID))
                                        overlapRngMax = int(max(setResultFFID))

                                        setOvrlpFFID = set(range(overlapRngMin,overlapRngMax+1 ) )

                                    if len(setResultFFID) > 0 or len(setOvrlpSP_FFID) > 0:
                                        #ffidSet : Set of overlap with FFID with postgres point
                                        # spSet : Set of overlap with source point with postgres point
                                        # ffidSet_sp : Set propotinate(respective) FFID of SP point overlap with postgres point
                                        clslog_log.writeLine("\n        Overlap Found :----- " + " with file " + seisFileName )
                                        if len(setResultFFID) > 0 :
                                            clslog_log.writeLine("\n        FFID  : " + str(min(setResultFFID)) + " --- " +  str(max(setResultFFID) ) )
                                        if len(setOvrlpSP) > 0:
                                            clslog_log.writeLine("\n        SPset : " + str(min(setOvrlpSP)) + " --- " + str(max(setOvrlpSP)))
                                            if len(setOvrlpSP_FFID) > 0:
                                                clslog_log.writeLine("\n        ffidSet_sp : " + str(min(setOvrlpSP_FFID)) + " --- " + str(max(setOvrlpSP_FFID)))

                                        if seisFileName not in dict_ResultOvlpRngs.keys():
                                            inputPath = XSDIR + '/' + eleFileRng['store_id'] + '/' + MEDIADIR + eleFileRng['location_reference'] + '/'

                                            dict_ResultOvlpRngs[seisFileName]= {'segment_id': segId, 'ffidSet': setOvrlpFFID , 'ffidSet_sp': setOvrlpSP_FFID , 'spSet': setOvrlpSP , 'remark': eleFileRng['remark'],'filePath': inputPath , 'filename':seisFileName , 'PointType': eleFileRng['point_type'] ,
                                                                                'headFormat' : eleFileRng["header_format"] , 'spFlg' : isSP_Flg , 'processing_name' : eleFileRng["processing_name"] , 'section_name' : eleFileRng["section_name"] , 'proc_set_type': eleFileRng["proc_set_type"]  , 'product_type' : eleFileRng["product_type"] }
                                        else:
                                            dict_ResultOvlpRngs[seisFileName]['ffidSet'] = dict_ResultOvlpRngs[seisFileName]['ffidSet'].union(setOvrlpFFID)
                                            dict_ResultOvlpRngs[seisFileName]['spSet'] = dict_ResultOvlpRngs[seisFileName]['spSet'].union(setOvrlpSP)
                                            dict_ResultOvlpRngs[seisFileName]['ffidSet_sp'] = dict_ResultOvlpRngs[seisFileName]['ffidSet_sp'].union(setOvrlpSP_FFID)
                                    else:
                                        clslog_log.writeLine("\n        No Overlap Found :----- " + " with file " + seisFileName)


                        ##################################################################################################################
                        clslog_log.writeLine("\n    ==============================================================================================================")
                        clslog_log.writeLine("\n\n    Processing Seis File Ranges Begin : " )
                        clslog_log.FlushFile()
                        # Process after processing all Ranges
                        if len(dict_ResultOvlpRngs) > 0 :
                            TotalSegOvlpFndInSeisFileCnt = TotalSegOvlpFndInSeisFileCnt + 1
                            set_TotalSegOvlpFndInSeisFile.add( segId)

                            for seisFileData in dict_ResultOvlpRngs.itervalues():
                                clslog_log.writeLine("\n    ----------------------------------------------------------------------------------------------------------")
                                clslog_log.writeLine("\n        Processing Seis File  :     " + seisFileData['filename'] )
                                if len ( seisFileData['spSet']) > 0:
                                    clslog_log.writeLine("\n        Processing File Range of Segment SP  :" +  " : " + str(min(seisFileData['spSet'])) + ' -- ' + str(max(seisFileData['spSet'])))
                                if len(seisFileData['ffidSet']) > 0:
                                    clslog_log.writeLine("\n        Processing File Range of Segment FFID:" + " : " + str(min(seisFileData['ffidSet'])) + ' -- ' + str(max(seisFileData['ffidSet'])))

                                # print "----------------------- Result is ----------------------------"
                                # print seisFileData['segment_id']
                                # print seisFileData['ffidSet']
                                # print seisFileData['remark']
                                vPointType = seisFileData['PointType']
                                srcFileName = seisFileData['filePath'] +  seisFileData['filename']
                                vProcName4Fldr = seisFileData["processing_name"]
                                vSectName4SubFldr = seisFileData["section_name"]
                                vProcSetType4SubFldr = seisFileData["proc_set_type"]
                                vProdTyp = seisFileData['product_type']

                                # Define 2d or 3d type folder
                                vType2dOr3d = Utilities.N_GetType2dOr3d(vProcSetType4SubFldr, vProdTyp)
                                #print vType2dOr3d;

                                if 'Error' in vType2dOr3d :
                                   #d.error = str(d.error) + 'Product Type not like 2D or 3D ;'
                                   print 'Product Type not like 2D or 3D ;'
                                   clslog_log.writeLine('\n         Product Type not like 2D or 3D ;')
                                   continue

                                # ------------------------------------------------------------------------------
                                #File not exists on defiened path then take Remark path
                                if not os.path.isfile(srcFileName):
                                    # Check filepath in remarks column
                                    #clslog_log.writeLine('\n>>' + srcFileName + ' is not found so set from remark  as ' + v["remark"])
                                    print('\n>>' + srcFileName + ' is not found so set from remark  as ' + seisFileData["remark"])
                                    srcFileName = seisFileData["remark"]

                                if not os.path.isfile(srcFileName):         # Local Path For Loacal Tsting Purpose
                                    srcFileName = "/home/raj/OGDR/Test_Sqy/"  + seisFileData['filename']
                                # ------------------------------------------------------------------------------

                                if not os.path.isfile(srcFileName):
                                    print srcFileName + ' Segy File not Found ;'
                                    #d.error = str(d.error) + srcFileName + ' Segy File not Found ;'
                                    #clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                    continue
                                else:
                                    # ------------------------------------------------------------------------------
                                    try:
                                        dPath = Utilities.Create_Dir_Structure(fldrNameBlkNoWKt, vProcName4Fldr,
                                                                               vProcSetType4SubFldr,
                                                                               vSectName4SubFldr, output_path, vType2dOr3d)
                                    except:
                                        print("Error : Unable to create Folder structure.")
                                        clslog_log.writeLine("\n        Error : Unable to create Folder structure.")
                                        # d.error = str(d.error) + 'Unable to create Folder structure for .' + fldrNameBlkNoWKt + '|' + vProcName4Fldr + '|' + vProcSetType4SubFldr + '|' + vSectName4SubFldr + '|' + output_path + '|' + vType2dOr3d
                                        clslog_log.writeLine('\n        Unable to create Folder structure for .' + fldrNameBlkNoWKt + '|' + vProcName4Fldr + '|' + vProcSetType4SubFldr + '|' + vSectName4SubFldr + '|' + output_path + '|' + vType2dOr3d)
                                        continue


                                    dstFileName = dPath + '/' + str(seisFileData['filename']).replace('.sgy', '') + '_'

                                    # dstFileName = dPath + '/' + str(seisFileData['filename']).replace('.sgy', '') + '_' + str(
                                    #     min(seisFileData['ffidSet'])) + '_' + str(
                                    #     max(seisFileData['ffidSet'])) + '_' + vPointType  + '.sgy'


                                    # ------------------------------------------------------------------------------
                                    # First Get Header type from ppdm dictionary information
                                    headFormat = seisFileData["headFormat"]
                                    vHeadPos = '';
                                    vheadType = '';

                                    vHeadPos, vheadType, errFlg = Utilities.N_GetHeaderFormat(clslog_log, dict_segyFormat, headFormat,seisFileName, segId )

                                    #print  vHeadPos, vheadType
                                    if (errFlg == True):
                                        continue;

                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        #d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        print "Error Reading in SegY File."
                                        clslog_log.writeLine("\n        Error Reading in SegY File.")
                                        #clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,"human_name": "raj"}

                                    if seisFileData['spFlg'] == True :
                                        if len(seisFileData['spSet']) > 0 :
                                            print min(seisFileData['spSet'])
                                            print max(seisFileData['spSet'])
                                            print seisFileData['spSet']


                                            dstFileName = dstFileName + str(min(seisFileData['spSet'])) + '_' + str(max(seisFileData['spSet'])) + '_' + vPointType + '.sgy'
                                            # Cut with SP point first
                                            f.polycut("test_raj", seisFileData['spSet'], dstFileName)

                                            # If spset overlap matchin not found then cut with Matching(respective)  FFID for that sp points (ffidSet_sp)
                                            if ( f.fileReadingStatus == 'UNSUCCESS' ) or ( f.fileReadingStatus == 'SUCCESS' and f.tr_written == 0 ) :
                                                #if 'Finding Trace number value 0' in f.fileReadingError:
                                                clslog_log.writeLine("\n        SP point not found with CDP number with <headpos> , So match FFID set with FFID with Position 8.")
                                                del f;
                                                f = segy.Segy(srcFileName)
                                                constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,"human_name": "raj"}
                                                f.polycut("test_raj", seisFileData['ffidSet_sp'], dstFileName)
                                                # else:
                                                #     constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,"human_name": "raj"}
                                                #     f.polycut("test_raj", seisFileData['ffidSet_sp'], dstFileName)

                                        # Not required as discusset with
                                        # # If no Matching found with sp points then  ffid overlap with sp points
                                        # elif len(seisFileData['ffidSet']) > 0 and spFoundFlg == False :
                                        #     print min(seisFileData['ffidSet'])
                                        #     print max(seisFileData['ffidSet'])
                                        #     print seisFileData['ffidSet']
                                        #
                                        #     dstFileName = dstFileName + str(min(seisFileData['ffidSet_sp'])) + '_' + str(
                                        #         max(seisFileData['ffidSet_sp'])) + '_' + vPointType + '.sgy'
                                        #     constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType, "human_name": "raj"}
                                        #     f.polycut("test_raj", seisFileData['ffidSet_sp'], dstFileName)

                                    else:
                                        if len(seisFileData['ffidSet']) > 0 :
                                            print min(seisFileData['ffidSet'])
                                            print max(seisFileData['ffidSet'])
                                            print seisFileData['ffidSet']

                                            clslog_log.writeLine("\n        Processing File Range of Segment     : " + seisFileData['segment_id'] + " : " + str(min(seisFileData['ffidSet'])) + ' -- ' + str(max(seisFileData['ffidSet'])))
                                            dstFileName = dstFileName + str(min(seisFileData['ffidSet'])) + '_' + str(max(seisFileData['ffidSet'])) + '_' + vPointType + '.sgy'

                                            f.polycut("test_raj", seisFileData['ffidSet'], dstFileName)



                                    if f.fileReadingStatus == 'SUCCESS':
                                        if f.tr_written > 0:
                                            #d.error = str(d.error) + ';' + "Traces Found."
                                            print "\nTraces Found."
                                            clslog_log.writeLine("\n        " +  str(f.tr_written)  +  " Traces Found.")
                                            clslog_log.writeLine("\n        File Created :" + str(f.tr_written) + dstFileName )
                                            #d.error = str(d.error) + ';' + "No Traces Found."
                                            TotalFileCnt = TotalFileCnt + 1

                                        else:
                                            print "\nNo Traces Found."
                                            clslog_log.writeLine("\n        " + str(f.tr_written) + " Traces Found.")
                                            clslog_log.FlushFile()
                        else:
                            clslog_log.writeLine("\n    No overlap Range found with seis file data.")
                            TotalNoOvrlpWithSiesFileDataCnt = TotalNoOvrlpWithSiesFileDataCnt + 1
                            set_TotalNoOvrlpWithSiesFileData.add(segId)
                    else:
                        clslog_log.writeLine("\n    No Range found.")
                        clslog_log.FlushFile()
                else:
                    clslog_log.writeLine("\n    No Seis file Range found              :    " + segId )
                    print("\n    No Seis file Range found              :    " + segId )
                    TotalSeisFileDataNotFndCnt = TotalSeisFileDataNotFndCnt + 1 ;
                    set_TotalSeisFileDataNotFnd.add(segId);
                    clslog_log.FlushFile()

            segment_id_set_subset.clear()



        #break     # For Single Eelement Main For Loop Break
    
    # Report summary begins.................Rajendra

    clslog_log.writeLine("\n\n-------------------------------------------------------------------------------------------------")
    clslog_log.FlushFile()
    clslog_log.writeLine("\nSegment List  not found 'fuseim_seis_match join fuseim_seis_file' :" + str(TotalSeisFileDataNotFndCnt) )
    if len (set_TotalSeisFileDataNotFnd)> 0:
        for segId_n in set_TotalSeisFileDataNotFnd:
            clslog_log.writeLine("\n\t" + segId_n)

    clslog_log.writeLine("\n\n-------------------------------------------------------------------------------------------------")
    clslog_log.writeLine("\nSegment List, points not found overlap with Seis file points      : " + str(TotalNoOvrlpWithSiesFileDataCnt) )
    if len (set_TotalNoOvrlpWithSiesFileData)> 0:
        for segId_n in set_TotalNoOvrlpWithSiesFileData:
            clslog_log.writeLine("\n\t" + segId_n)

    clslog_log.writeLine("\n\n-------------------------------------------------------------------------------------------------")
    clslog_log.writeLine("\nOverlap Segment id Processed Succesfully !")
    setr = segment_id_set.difference(set_TotalNoOvrlpWithSiesFileData)
    setProcessed = setr.difference(set_TotalSeisFileDataNotFnd)
    if len (setProcessed)> 0:
        for segId_n in setProcessed:
            clslog_log.writeLine("\n\t" + segId_n)


    # clslog_log.writeLine(
    #     "\n\n-------------------------------------------------------------------------------------------------")
    # clslog_log.writeLine("\nSegment id does not intersect with block ")
    # setr = segment_id_set.difference(set_TotalNoGisIntersectionNotFndCnt)
    # setProcessed = setr.difference(set_TotalNoGisIntersectionNotFndCnt)
    # if len(setProcessed) > 0:
    #     for segId_n in setProcessed:
    #         clslog_log.writeLine("\n\t" + segId_n)

    clslog_log.writeLine("\n\n-------------------------------------------------------------------------------------------------")

    clslog_log.writeLine("\n\n******************************* Completed Successfully ! ***********************************")
    clslog_log.writeLine("\n\n******************************* Summary ***********************************")
    clslog_log.writeLine("\nOther than BIN Segments Totally Inside : " + str( len(segment_id_otr_BIN_WithIn_set)) )
    clslog_log.writeLine("\nOther than BIN Segements Overlap       : " + str( len(segment_id_set_otherThanBinOverlap)) )
    
    clslog_log.writeLine("\nBIN Segments Totally Inside : " + str( len(segment_id_BIN_WithIn_set)) )
    clslog_log.writeLine("\nBIN Segements Overlap       : " + str( len(segment_id_set_bin_overlap)) )
    
    clslog_log.writeLine("\n\nSegments Totally inside (With In) in Area : " + str( len(segment_id_otr_BIN_WithIn_set)) + " + " + str( len(segment_id_BIN_WithIn_set)) + " ) = " + str(len(segment_id_WithIn_set)) )
    
    clslog_log.writeLine("\n\nSegments Overlap  with Area :( " + str( len(segment_id_set_otherThanBinOverlap))+ " + " + str( len(segment_id_set_bin_overlap))+ " ) = "+ str(len(segment_id_set)))
   
    clslog_log.writeLine("\nOverlap summary details :-------------- " + str(TotalSegmentCnt))
    clslog_log.writeLine("\n    Total Segment overlap found with Seis file                               : " + str(TotalSegOvlpFndInSeisFileCnt))
    clslog_log.writeLine("\n    Total Segment points not found overlap with Seis file points             : " + str(TotalNoOvrlpWithSiesFileDataCnt))
    clslog_log.writeLine("\n    Total Segment record not found 'fuseim_seis_match join fuseim_seis_file' : " + str(TotalSeisFileDataNotFndCnt))
    clslog_log.writeLine("\n    Total file created from overlap segemnt : " + str(TotalFileCnt))
    clslog_log.writeLine("\n\n-------------------------------------------------------------------------------------------------")

    TotalSegmentPrcosed =   len(segment_id_set) + len(segment_id_WithIn_set) 
    clslog_log.writeLine("\nTotal segment  processes : (" + str(len(segment_id_set)) + " + " + str(len(segment_id_WithIn_set)) + ") = " + str(TotalSegmentPrcosed))
    TotalAllFileCnt = copyFileCnt4WithIn + copyFileCnt + TotalFileCnt
    clslog_log.writeLine("\nTotal file created : ("+ str(copyFileCnt4WithIn) + "+" + str(copyFileCnt) + "+" + str(TotalFileCnt) + ")=" + str(TotalAllFileCnt) )

    if copyFileCnt > 0:
        clslog_log.writeLine("\nNote: Please copy files which are more 1 GB, by running .sh on server")

    clslog_log.writeLine("\n*******************************        Thanks !!        ***********************************")
    clslog_log.writeLine ("\n*********************  Developed By: Target Oilfields Services LLC. ***********************\n")
    
    clslog_log.writeLine("\nProcess Start Time :    " + str(start_time) + "\n" )
    clslog_log.writeLine("\nProcess End Time   :    %s" % strftime("%Y-%m-%d %H:%M:%S", localtime()))
    clslog_log.closefile()

    # For console printing ....................Rajendra

    print("\n******************************* Completed Successfully ! ***********************************")
    print("*******************************        Thanks !!        ***********************************")
    print ("*********************  Developed By: Target Oilfields Services LLC. ***********************\n")
    print("\n\n************************************ Summary *****************************************")
    
    print("\nOther than BIN Segments Totally Inside : " + str( len(segment_id_otr_BIN_WithIn_set)) )
    print("\nOther than BIN Segements Overlap       : " + str( len(segment_id_set_otherThanBinOverlap)) )
    
    print("\nBIN Segments Totally Inside : " + str( len(segment_id_BIN_WithIn_set)) )
    print("\nBIN Segements Overlap       : " + str( len(segment_id_set_bin_overlap)) )
    
    print("\n\nSegments Totally inside (With In) in Area : " + str( len(segment_id_otr_BIN_WithIn_set)) + " + " + str( len(segment_id_BIN_WithIn_set)) + " ) = " + str(len(segment_id_WithIn_set)))
    
    print("\n\nSegments Overlap  with Area :( " + str( len(segment_id_set_otherThanBinOverlap))+ " + " + str( len(segment_id_set_bin_overlap))+ " ) = "+ str(len(segment_id_set)))
    
    print("\nOverlap summary details :-------------- " + str(TotalSegmentCnt))
    print("\n    Total Segment overlap found with Seis file                               : " + str(TotalSegOvlpFndInSeisFileCnt))
    print("\n    Total Segment points not found overlap with Seis file points             : " + str(TotalNoOvrlpWithSiesFileDataCnt))
    print("\n    Total Segment record not found 'fuseim_seis_match join fuseim_seis_file' : " + str(TotalSeisFileDataNotFndCnt))
    print("\n    Total file created from overlap segemnt : " + str(TotalFileCnt))
    print("\n\n-------------------------------------------------------------------------------------------------")

    TotalSegmentPrcosed =   len(segment_id_set) + len(segment_id_WithIn_set) 
    print("\nTotal segment  processes : (" + str(len(segment_id_set)) + " + " + str(len(segment_id_WithIn_set)) + ") = " + str(TotalSegmentPrcosed))
    TotalAllFileCnt = copyFileCnt4WithIn + copyFileCnt + TotalFileCnt
    print("\nTotal file created : (" + str(copyFileCnt4WithIn) + "+" + str(copyFileCnt) + "+" + str(TotalFileCnt) + ")=" + str(TotalAllFileCnt) )

    if copyFileCnt > 0:
        print("\nNote: Please copy files which are more 1 GB, by running .sh on server")

    print("\n************************************ The End *****************************************")
    print("\nProcess Start Time :    " + str(start_time) + "\n" )
    print("\nProcess End Time   :    %s" % strftime("%Y-%m-%d %H:%M:%S", localtime()))
    
# Main Programm Call
if __name__ == '__main__':
    IsDebugTest = False

    if IsDebugTest == False:
        set_parameters()
    else:
        CallMainProgram( 1 , IsDebugTest )

    # -------------------------------------------------------------------------
    # Test Call for functions
    # remin, remax = N_GetFFidFromSpPoints(500, 558, 520, 558, 500, 558, False, False)
    # remin, remax = N_GetFFidFromSpPoints(560, 644, 560, 644, 560, 644, False, False)
    # remin, remax = N_GetFFidFromSpPoints(646, 683, 646, 683, 646, 683, False, False)
    # remin, remax = N_GetFFidFromSpPoints(700, 734, 710, 720, 700, 734, False, False)
    # remin, remax = N_GetFFidFromSpPoints(300,427,230,350,230,357 ,True, False)
    # remin, remax = N_GetFFidFromSpPoints(429, 499, 158, 228, 158, 228, True, False)
    # remin, remax = N_GetFFidFromSpPoints(500, 541, 140, 157, 116, 157, True, False)
    # remin, remax = N_GetFFidFromSpPoints(500, 541, 140, 150, 116, 157, True, False)
    # print remin, remax;
    # exit(0)
    # For Header value testing :
    # vHeadPos, vheadType, errFlg = N_GetHeaderFormat(clslog_log, dict_segyFormat, "PRESTACK-3", "test.sgy")







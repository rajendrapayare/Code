import os
import sys
import pymssql
import argparse
import shutil
import time
from time import gmtime, strftime , localtime
import csv
import psycopg2
import psycopg2.extras
from PostgreSQL import PostgreSQL
import xlsxwriter
import traceback
import TempLateClasses

SERVER_CREATE_CSV_PATH = '/home/'  # Path to create csv file of database list points which we can to avoid query on databases

### Some Patrameters for local testing..

#G_DICT_MIN_MAX_NAME = '/home/raj/OGDR/Test_csv/dict_min_max_test_case1.csv'
#G_DICT_MIN_MAX_NAME = '/home/raj/OGDR/Test_csv/dict_min_max_test_case_duplicate.csv'
#G_DICT_MIN_MAX_NAME = '/home/raj/OGDR/Test_csv/New/CASE_4/dict_MinMax_FromDB_31.csv'
#G_DICT_MIN_MAX_NAME = '/home/raj/OGDR/Test_csv/New/ALL_datasheet/dict_MinMax_FromDB_31.csv'
G_DICT_MIN_MAX_NAME = '/home/raj/OGDR/Test_csv/block17/case1/dict_MinMax_FromDB.csv'

#G_DICT_GEOM ='/home/raj/OGDR/Test_csv/New/dict_geom_test_FromDB_31.csv'
#G_DICT_GEOM ='/home/raj/OGDR/Test_csv/New/SEGD/TestCase_dict_geom_test_FromDB_31_SEGD.csv'
#G_DICT_GEOM ='/home/raj/OGDR/Test_csv/New/CASE_4/TestCase_dict_geom_test_FromDB_31_SEGD.csv'
#G_DICT_GEOM ='/home/raj/OGDR/Test_csv/New/ALL_datasheet/TestCase_dict_geom_test_FromDB_31_ALL.csv'
G_DICT_GEOM ='//home/raj/OGDR/Test_csv/block17/case1/dict_geom_test_FromDB.csv'

#G_DICT_GEOM_POINTS ='/home/raj/OGDR/Test_csv/New/dict_geom_Pnts_FromDB.csv'
G_DICT_GEOM_POINTS ='/home/raj/OGDR/Test_csv/block17/dict_geom_Pnts_FromDB.csv'


# G_DICT_PPDM ='/home/raj/OGDR/Test_csv/dict_ppdm_test_case_pntRangeMismtch.csv'
# G_DICT_PPDM ='/home/raj/OGDR/Test_csv/New/SEGD/TestCase_dict_ppdm_FromDB_31_SegD.csv'
#G_DICT_PPDM = '/home/raj/OGDR/Test_csv/New/CASE_4/TestCase_dict_ppdm_FromDB_31_SegD.csv'
#G_DICT_PPDM ='/home/raj/OGDR/Test_csv/New/ALL_datasheet/TestCase_dict_ppdm_FromDB_31_ALL.csv'
G_DICT_PPDM ='/home/raj/OGDR/Test_csv/block17/case1/dict_ppdm_FromDB.csv'

######################################################################################

def pgsql_connect():
    print("Wait Creating Postgres Connection !!")
    return psycopg2.connect(dbname=PGDB, user=PGUSER, host=PGCONN, password=PGPASS)

def pgsql_cursor(conn):
    return conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

def execute_query(cursor, query):
    return cursor.execute(query)

def pgsql_connect_close(conn):
    return conn.close()

def GetListOfPntDiffOffset(pPgsql_conn, pSeg_id,pPnt ):

    #This flage is default false
    # It will set true in following cases like
    #    if point count is less in 8
    #    some major range difference

    NotInSequencialRanageFlg = False
    pointCountInDb = 7;
    changeOffsetFoundDefaultCntr = 4;

    query4PntDiff ="select segment_id,point from seis_segment_geom where " \
                 "segment_id in ('{0}') and point >= {1} order by point limit 10;".format(pSeg_id, pPnt )

    print query4PntDiff
    print "\n%s - Request spatial database for Points offset.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    pgsql_cursor4 = pPgsql_conn.cursor()
    pgsql_cursor4.execute(query4PntDiff)
    res = pgsql_cursor4.fetchall()
    cnter = 0
    listPnt= []

    pntDiffOffset = 1
    changeOffsetFoundCntr = 0;

    for row in res:
        listPnt.append(int(row[1]))

        if cnter == 1 :
            pntDiffOffset = (listPnt[cnter] - listPnt[cnter - 1] )
        elif cnter > 1:
            if (listPnt[cnter] - listPnt[cnter-1] ) < pntDiffOffset:
                pntDiffOffset = (listPnt[cnter] - listPnt[cnter -1 ] )
                changeOffsetFoundCntr += 1

        cnter += 1
    pgsql_cursor4.close()

    if (cnter < pointCountInDb) or ( changeOffsetFoundCntr > changeOffsetFoundDefaultCntr ) :
        NotInSequencialRanageFlg = True

    print "\n%s - Finished: Request spatial database for Points offset.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return NotInSequencialRanageFlg, pntDiffOffset

def GetInterSectPoint(pgsql_conn,pIinsideLength, pSeg_id, pSegSpStart, pSegSpEnd):
    intPntXYwithSRID = ''

    # global queryTxt4Pnt, intPntXYwithSRID, pgsql_cursor4, res, row

    queryTxt4Pnt = "SELECT ST_AsEWKT(ST_Line_Interpolate_Point(the_line,cast( {0} as float(53)) ) )" \
                   " from (select ST_MakeLine( (SELECT the_geom FROM seis_segment_geom " \
                   "WHERE segment_id = '{1}'  AND POINT={2})," \
                   "(SELECT the_geom FROM seis_segment_geom WHERE " \
                   "segment_id ='{3}' AND POINT={4} ) " \
                   ") as the_line ) as foo;".format(pIinsideLength, pSeg_id, pSegSpStart, pSeg_id,
                                                    pSegSpEnd)
    intPntXYwithSRID = ''

    print queryTxt4Pnt
    print "\n%s - Request spatial database for Intersection.." % strftime("%Y-%m-%d %H:%M:%S", localtime())

    pgsql_cursor4 = pgsql_conn.cursor()
    pgsql_cursor4.execute(queryTxt4Pnt)
    res = pgsql_cursor4.fetchall()
    for row in res:
        #print row[0]
        intPntXYwithSRID = str(row[0])
    pgsql_cursor4.close()

    print "\n%s - Finished: Request spatial database for Intersection.." % strftime("%Y-%m-%d %H:%M:%S", localtime())

    return intPntXYwithSRID

def GetListOfPntRangeInside4SegmentId(pSeg_id, pWktString):

    query4Pnts ="select point from seis_segment_geom where ST_Within(the_geom, ST_GeomFromText('{0} ,4326)) " \
                 "and segment_id in ('{1}') order by point;".format(pWktString,pSeg_id )

    print query4Pnts
    print "\n%s - Request Point Range from PostGIS database for Points.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    pgsql_cursor4 = pgsql_conn.cursor()
    pgsql_cursor4.execute(query4Pnts)
    res = pgsql_cursor4.fetchall()
    listPnt =[]
    cnter = 0

    pntDiffOffset = 0

    for row in res:
        print row[0]
        listPnt[cnter] = int(row[0])

        # if cnter > 1 :
        #     if (listPnt[cnter-1] - listPnt[cnter] ) < pntDiffOffset:
        #         pntDiffOffset = (listPnt[cnter-1] - listPnt[cnter] )
        # elif cnter == 1 :
        #     pntDiffOffset = (listPnt[cnter-1] - listPnt[cnter] )
        #
        # cnter = cnter + 1

    pgsql_cursor4.close()

    print "\n%s - Finished: Request Point Range from PostGIS database for Points.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    #return listPnt,pntDiffOffset
    return listPnt

def GetPntStartOfRespectiveFFID(pMsSqlConn, pSeg_id, pMinFFID, pMaxFFID):

    mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
    #query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format(
    query = "select min(point_start) min_point_start, max(point_start) max_point_start from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
        pSeg_id, pMinFFID, pMaxFFID)

    spPointMin = ''
    spPointMax = ''

    print query
    print "\n%s - Request PPDM database for SP Points of Segment using FFID.." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                   localtime())
    mssql_cursor1.execute(query)
    for row in mssql_cursor1:
        spPointMin = row['min_point_start']
        spPointMax = row['max_point_start']
        break

    del mssql_cursor1

    print "\n%s - Finished: Request PPDM database for SP Points of Segment using FFID.." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                              localtime())
    return spPointMin, spPointMax

def GetInsideLength(pPgsql_conn, pWkt, pSeg_id, pSegSpStart, pSegSpEnd, pSrid=None):

    insideLength = 0.0;

    queryTxtInt = "select ST_LENGTH( " \
                  "ST_Intersection( ST_GeomFromText('{0}',{5})," \
                  " ST_MakeLine(" \
                  "(SELECT the_geom FROM seis_segment_geom WHERE segment_id = '{1}'  AND POINT={2}), " \
                  "(SELECT the_geom FROM seis_segment_geom WHERE segment_id ='{3}' AND POINT={4}) )" \
                  ") " \
                  ") ".format(pWkt, pSeg_id, pSegSpStart, pSeg_id, pSegSpEnd, pSrid)

    print queryTxtInt
    print "\n%s - Request spatial database for Inside Segment Length." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    pgsql_cursor4 = pPgsql_conn.cursor()
    pgsql_cursor4.execute(queryTxtInt)
    res = pgsql_cursor4.fetchall()
    for row in res:
        print row[0]
        insideLength = float(row[0])
    pgsql_cursor4.close()

    print "\n%s - Finished: Request spatial database for Inside Segment Length." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return insideLength

def GetSegmentTotalDistance(pPgsql_conn, pSeg_id, pSegSpStart, pSegSpEnd):

    total_length = 0.0
    queryTxt = "select ST_DISTANCE((SELECT the_geom FROM seis_segment_geom WHERE segment_id = '{0}' AND POINT={1})," \
               "(SELECT the_geom FROM seis_segment_geom WHERE segment_id = '{2}'  AND POINT= {3})) ".format(
        pSeg_id, pSegSpStart, pSeg_id, pSegSpEnd)

    # polygon_type = input_polygon_type()
    try:

        print queryTxt
        print "\n%s - Request spatial database for Segment Distnace(Length)." % strftime("%Y-%m-%d %H:%M:%S", localtime())
        pgsql_cursor3 = pPgsql_conn.cursor()
        pgsql_cursor3.execute(queryTxt)
        res = pgsql_cursor3.fetchall()
        for row in res:
            print row[0]
            total_length = float(row[0])

        pgsql_cursor3.close()

    except:
        #print("Error : No Record Geometry Record found in database fro seg_id: %s", seg_id)
        total_length = -1

    print "\nFinished: %s - Request spatial database for Segment Distnace (Length)." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return  total_length

def GetFFIdpntsFromMinMax_SqlDB_bakup(pMsSqlConn, pSeg_id,pMinpnt, pMaxPnt, pSectionName, pSeis_file_id ):

    spStartAsFfidFlg = False        # This Flag is set true when sp_start entry not found in database and run query against ffid_start

    mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
    # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format(pSeg_id, pMinpnt, pMaxPnt)
    #query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format (pSeg_id, pMinpnt, pMaxPnt)
    query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and sp_start >= {2} and sp_start <= {3} and seis_file_id ='{4}'".format(
        pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)

    ffidMin =''
    ffidMax = ''

    print query
    print "\n%s - Request PPDM database for MIN MAX Points of Segment using Sp_Start.." % strftime("%Y-%m-%d %H:%M:%S", localtime())

    mssql_cursor1.execute(query)
    for row in mssql_cursor1:
        ffidMin = row['minffid']
        ffidMax = row['maxffid']
        break

    if( ffidMin=='' or ffidMin == None) and ( ffidMax=='' or ffidMax == None):
        # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
        #     pSeg_id, pMinpnt, pMaxPnt)
        query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and ffid_start >= {2} and ffid_start <= {3} and seis_file_id ='{4}'".format(
            pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)
        print query
        print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                        localtime())
        mssql_cursor1.execute(query)
        for row in mssql_cursor1:
            ffidMin = row['minffid']
            ffidMax = row['maxffid']
            break

        spStartAsFfidFlg = True

    del mssql_cursor1

    print "\n%s - Finished: Request PPDM database for MIN MAX Points of Segment.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return spStartAsFfidFlg, ffidMin, ffidMax

# def GetFFIdpntsFromMinMax_SqlDB(pMsSqlConn, pSeg_id,pMinpnt, pMaxPnt, pSectionName, pSeis_file_id ):
#
#     spStartAsFfidFlg = 0        # This Flag is set true when sp_start entry not found in database and run query against ffid_start
#
#     mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
#     # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format(pSeg_id, pMinpnt, pMaxPnt)
#     #query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format (pSeg_id, pMinpnt, pMaxPnt)
#     query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and sp_start >= {2} and sp_start <= {3} and seis_file_id ='{4}'".format(
#         pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)
#
#     ffidMin =''
#     ffidMax = ''
#
#     print query
#     print "\n%s - Request PPDM database for MIN MAX Points of Segment using Sp_Start.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
#
#     mssql_cursor1.execute(query)
#     for row in mssql_cursor1:
#         ffidMin = row['minffid']
#         ffidMax = row['maxffid']
#         break
#
#     if( ffidMin=='' or ffidMin == None) and ( ffidMax=='' or ffidMax == None):
#         # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
#         #     pSeg_id, pMinpnt, pMaxPnt)
#         query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and ffid_start >= {2} and ffid_start <= {3} and seis_file_id ='{4}'".format(
#             pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)
#         print query
#         print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime("%Y-%m-%d %H:%M:%S",
#                                                                                         localtime())
#         mssql_cursor1.execute(query)
#         for row in mssql_cursor1:
#             ffidMin = row['minffid']
#             ffidMax = row['maxffid']
#             break
#
#         spStartAsFfidFlg = 1
#
#     #Added  for Propotion calculation
#     if (ffidMin == '' or ffidMin == None) or (ffidMax == '' or ffidMax == None):
#         # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
#         #     pSeg_id, pMinpnt, pMaxPnt)
#         query1 = "select min(point_start) minpntst, max(point_end) maxpntend , min(ffid_start) minffid, max(ffid_end ) maxffid  from fuseim_seis_match where geom_segment_id ='{0}' and section_name='{1}' and segment_status != 5  and seis_file_id ='{2}' ".format(
#             pSeg_id, pSectionName,  pSeis_file_id)
#
#         print query1
#         print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime(
#             "%Y-%m-%d %H:%M:%S",
#             localtime())
#         mssql_cursor1.execute(query1)
#         for row in mssql_cursor1:
#             ffidMin = row['minffid']
#             ffidMax = row['maxffid']
#             minpntst = row['minpntst']
#             maxpntend = row['maxpntend']
#             break
#
#         diff =    ( int(ffidMax) - int (ffidMin ) ) / ( int(maxpntend) - int (minpntst ) )
#         ffidMin = str (int(( round ( int(pMinpnt ) * diff , 0 ) )))
#         ffidMax = str (int(( round ( int(pMaxPnt) * diff , 0 ) )))
#
#         spStartAsFfidFlg = 2
#
#     del mssql_cursor1
#
#     print "\n%s - Finished: Request PPDM database for MIN MAX Points of Segment.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
#     return spStartAsFfidFlg, ffidMin, ffidMax

def GetFFIdpntsFromMinMax_SqlDB(pMsSqlConn, pSeg_id,pMinpnt, pMaxPnt, pSectionName, pSeis_file_id ):

    spStartAsFfidFlg = 0        # This Flag is set true when sp_start entry not found in database and run query against ffid_start

    mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
    # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format(pSeg_id, pMinpnt, pMaxPnt)
    #query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format (pSeg_id, pMinpnt, pMaxPnt)
    query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and sp_start >= {2} and sp_start <= {3} and seis_file_id ='{4}'".format(
        pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)

    ffidMinret =''
    ffidMaxret = ''

    print query
    print "\n%s - Request PPDM database for MIN MAX Points of Segment using Sp_Start.." % strftime("%Y-%m-%d %H:%M:%S", localtime())

    mssql_cursor1.execute(query)
    for row in mssql_cursor1:
        ffidMinret = row['minffid']
        ffidMaxret = row['maxffid']
        break

    if( ffidMinret=='' or ffidMinret == None) and ( ffidMaxret=='' or ffidMaxret == None):
        # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
        #     pSeg_id, pMinpnt, pMaxPnt)
        query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and ffid_start >= {2} and ffid_start <= {3} and seis_file_id ='{4}'".format(
            pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)
        print query
        print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                        localtime())
        mssql_cursor1.execute(query)
        for row in mssql_cursor1:
            ffidMinret = row['minffid']
            ffidMaxret = row['maxffid']
            break

        spStartAsFfidFlg = 1

    #Added  for Propotion calculation
    if (ffidMinret == '' or ffidMinret == None) or (ffidMaxret == '' or ffidMaxret == None):
        # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and ffid_start >= {1} and ffid_start <= {2}".format(
        #     pSeg_id, pMinpnt, pMaxPnt)
        query1 = "select min(point_start) minpntst, max(point_end) maxpntend , min(ffid_start) minffid, max(ffid_end ) maxffid  from fuseim_seis_match where geom_segment_id ='{0}' and section_name='{1}' and segment_status != 5  and seis_file_id ='{2}' ".format(
            pSeg_id, pSectionName,  pSeis_file_id)

        print query1
        print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime(
            "%Y-%m-%d %H:%M:%S",
            localtime())
        mssql_cursor1.execute(query1)

        ffidMin = ''
        ffidMax = ''

        for row in mssql_cursor1:
            ffidMin = row['minffid']
            ffidMax = row['maxffid']
            minpntst = row['minpntst']
            maxpntend = row['maxpntend']
            break

        #diff =    round ( ( int(ffidMax) - int (ffidMin ) ) / float( ( int(maxpntend) - int (minpntst ) ) ), 0)
        if float(abs (int(maxpntend) - int(minpntst))) != 0 :
            diff = (int(ffidMax) - int(ffidMin)) / float((int(maxpntend) - int(minpntst)))
            aa = []
            aa = str(diff).split('.')
            pre1 = '0.' + aa[1]

            preFl = float(pre1)
            if( preFl> 0.9  ) or (preFl > 0.4  and preFl < 0.5 ):
                  diff = round(diff, 1 )

            ffidMinret = int(ffidMin) + int(( round  ( ( int(pMinpnt ) - int(minpntst) ) * diff , 0 ) ))
            ffidMaxret = int(ffidMin) + int(( round  ( ( int(pMaxPnt ) - int(minpntst) ) * diff , 0 ) ))

            #put if case for if stpnt matches with ffid start and endpnt matches with ffid end
            if str(minpntst) == str(pMinpnt):
                ffidMinret = ffidMin

            if str(maxpntend) == str(pMaxPnt):
                ffidMaxret = ffidMax

            spStartAsFfidFlg = 2

    del mssql_cursor1

    print "\n%s - Finished: Request PPDM database for MIN MAX Points of Segment.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return spStartAsFfidFlg, ffidMinret, ffidMaxret

def GetFFIdpntsFromMinMax_SqlDB4SEGD(pMsSqlConn, pSeg_id, pSectionName , pSeis_file_id, pMinpnt, pMaxPnt):

    spStartAsFfidFlg = False        # This Flag is set true when sp_start entry not found in database and run query against ffid_start

    mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
    # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and sp_start >= {1} and sp_start <= {2}".format (pSeg_id, pMinpnt, pMaxPnt)
    # query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and sp_start >= {2} and sp_start <= {3} and seis_file_id ='{4}' ".format(
    #     pSeg_id, pSectionName, pMinpnt, pMaxPnt ,pSeis_file_id  )
    query = "select min(ffid_start) minffid, max(ffid_end ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and section_name='{1}' and sp_start >= {2} and sp_start <= {3} and seis_file_id ='{4}' ".format(
        pSeg_id, pSectionName, pMinpnt, pMaxPnt, pSeis_file_id)

    ffidMin =''
    ffidMax = ''

    print query
    print "\n%s - Request PPDM database for MIN MAX Points of Segment using Sp_Start.." % strftime("%Y-%m-%d %H:%M:%S", localtime())

    mssql_cursor1.execute(query)
    for row in mssql_cursor1:
        ffidMin = row['minffid']
        ffidMax = row['maxffid']
        break

    if( ffidMin=='' or ffidMin == None) and ( ffidMax=='' or ffidMax == None):
        query = "select min(ffid_start) minffid, max(ffid_start ) maxffid from fuseim_seis_match where geom_segment_id = '{0}' and  section_name='{1}' and ffid_start >= {2} and ffid_start <= {3} and seis_file_id ='{4}' ".format(
            pSeg_id, pSectionName,  pMinpnt, pMaxPnt, pSeis_file_id)
        print query
        print "\n%s - Request PPDM database for MIN MAX Points of Segment using FFID_START.." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                        localtime())
        mssql_cursor1.execute(query)
        for row in mssql_cursor1:
            ffidMin = row['minffid']
            ffidMax = row['maxffid']
            break

        spStartAsFfidFlg = True

    del mssql_cursor1

    print "\n%s - Finished: Request PPDM database for MIN MAX Points of Segment.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    return spStartAsFfidFlg, ffidMin, ffidMax

# Added path , block number, server path variable pSERVER_CREATE_CSV_PATH on 18 Nov 2018
#def Get_Dict_DictGisStEnd( pPgsqlConn, pSegment_id_set , pDICT_MINMAX_FROM_CSV , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG ):
def Get_Dict_DictGisStEnd(pPgsqlConn, pSegment_id_set, pDICT_MINMAX_FROM_CSV, pSERVER_TEST_FLG,
                              pSERVER_CREATE_CSV_FLG , pSERVER_CREATE_CSV_PATH , pPolygonType , pBlockNumber ):

    dict_gis_st_end_ret = {}

    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    set_size = 10000
    total_queryCouunt = len(pSegment_id_set) / set_size

    #Comment this while local run with csv file
    #pSERVER_TEST_FLG = True  # For Server query fetch  test on with local run  oterwise comment it.

    if not pDICT_MINMAX_FROM_CSV:
        for seg in pSegment_id_set:
            seg_nb += 1
            seg_query += 1
            segment_id_set_string1 += "\'%s\'," % seg

            if pSERVER_TEST_FLG == True:
                if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                    segment_id_set_string1 = segment_id_set_string1[:-1]
                    query_count += 1
                    print query_count

                    query = "select segment_id, min(point) as g_seg_stPnt, max(point) as g_seg_endPnt  from seis_segment_geom where segment_id in(%s) group by segment_id;" % \
                            segment_id_set_string1

                    # print ("\n %s - Request spatial database for min max point....Query Count %d / %d ." , strftime("%Y-%m-%d %H:%M:%S",
                    #                                                                            localtime()) , query_count , total_queryCouunt )
                    print ("\n " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Request spatial database for min max point....Query Count " + str(query_count) + " / " + str(total_queryCouunt) )
                    print query
                    pgsql_cursor2 = pPgsqlConn.cursor()
                    pgsql_cursor2.execute(query)
                    res = pgsql_cursor2.fetchall()

                    if pSERVER_CREATE_CSV_FLG == True:
                        # fsv4 = open('/home/raj/OGDR/Test_csv/New/dict_MinMax_FromDB.csv', 'w+')
                        # fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt')

                        if pPolygonType == 1:
                            fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_BLK' + str(pBlockNumber) + '.csv', 'w+')
                        else:
                            fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_' + 'WKT' + '.csv', 'w+')

                        fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt')

                    for row in res:
                        row_count += 1
                        if row[0] not in dict_gis_st_end_ret.keys():
                            dict_gis_st_end_ret[row[0]] = {'segment_id': row[0], 'g_seg_stPnt': row[1],'g_seg_endPnt': row[2]}

                            if pSERVER_CREATE_CSV_FLG == True:
                                fsv4.write('\n' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]))

                    del pgsql_cursor2

        if pSERVER_CREATE_CSV_FLG == True:
            fsv4.close()

            # print("\n Finished: %s - Result return for query count %d / %d \n" , (strftime("%Y-%m-%d %H:%M:%S", localtime())) , query_count , total_queryCouunt)
            print ("\n Finished: " + (strftime("%Y-%m-%d %H:%M:%S",
                                     localtime())) + " - Request spatial database for min max point....Query Count " + str(
                query_count) + " / " + str(total_queryCouunt))


    else:
        ##################################################################################
        # When Ignoring Database Query Part
        ##################################################################################
        with open(G_DICT_MIN_MAX_NAME , 'r') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                if row['segment_id'] not in dict_gis_st_end_ret.keys():
                    dict_gis_st_end_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                          'g_seg_stPnt': row['g_seg_stPnt'],
                                                          'g_seg_endPnt': row['g_seg_endPnt']}

            del reader

    return dict_gis_st_end_ret



# Added processType_ProcessFieldAll parameter on 01 - Oct -2018
# Added SERVER_CREATE_CSV_PATH parameter on 15 - Nov -2018
#def Get_Dict_DictGeom_and_DictGeomNew( pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV  , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG , pBlockNumber , pPolygonType):
#def Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV, pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType , pProcessTypeFlg):
#def Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV, pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType, pProcessTypeFlg,pProductTypeFlg ):
def Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV, pSERVER_TEST_FLG,
                                          pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType, pProcessTypeFlg,
                                          pProductTypeFlg , pSERVER_CREATE_CSV_PATH ):
    dict_geom_new_ret = {}
    dict_geom_ret ={}
    row_count = 0;

    query = ""

    try:
        if not pDICT_GEOM_FROM_CSV:
            if pSERVER_TEST_FLG == True:
                # Points in Polygon
                # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                if pPolygonType == 1:

                    # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                    #         "where ST_Within(the_geom, (select the_geom from concessions where license1 like 'BLOCK %s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                    if (pProcessTypeFlg == 'ALL'):
                        if pProductTypeFlg == 'ALL':
                            query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where ST_Within(the_geom, (select geom from concessions where blocknumber ='%s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                        else:
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id".format(pProductTypeFlg, pBlockNumber)
                    elif (pProcessTypeFlg == 'PROCESS'):
                        # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                        #         "where location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select the_geom from concessions where license1 like 'BLOCK %s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                        if pProductTypeFlg == 'ALL' :
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '%s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                        else:
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id".format(pProductTypeFlg, pBlockNumber)
                    elif (pProcessTypeFlg == 'FIELD'):
                        # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                        #         "where location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select the_geom from concessions where license1 like 'BLOCK %s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                        if pProductTypeFlg == 'ALL':
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '%s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                        else:
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id".format(pProductTypeFlg, pBlockNumber)

                    #----------------------------------------------------------------------------------------------------
                    # Testing Query ..Put segement id for specific segemnt
                    # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                    #         "where ST_Within(the_geom, (select geom from concessions where blocknumber like '%s' limit 1 ) ) and segment_id in ('OXY96SAB1-Other-SW106-1') group by segment_id order by segment_id" % pBlockNumber
                    # # TEsting Query End
                    # ----------------------------------------------------------------------------------------------------

                else:
                    if (pProcessTypeFlg == 'ALL'):
                        if pProductTypeFlg == 'ALL':
                            query = "select segment_id, count(*) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt
                        else:
                            query = "select segment_id, count(*) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id".format(pProductTypeFlg, pWkt)

                    elif (pProcessTypeFlg == 'PROCESS'):
                        if pProductTypeFlg == 'ALL':
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt
                        else:
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id" .format(pProductTypeFlg, pWkt)
                    elif (pProcessTypeFlg == 'FIELD'):
                        if pProductTypeFlg == 'ALL':
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt
                        else:
                            query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                    "where product_type like '{0}%' and location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id".format(pProductTypeFlg, pWkt)

                # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                print query

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                res = execute_query(pgsql_cursor, query)
                res = pgsql_cursor.fetchall()

                dict_geom_ret1 = res

                print "\n%s - Finished: Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                          localtime())
                print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

                fsv = None;

                if pSERVER_CREATE_CSV_FLG == True:
                    # fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List.csv', 'w+')
                    if pPolygonType == 1:
                        fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_BLK' + str(pBlockNumber) + '.csv', 'w+')
                    else:
                        fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_' + 'WKT' + '.csv', 'w+')

                    fsv.write('segment_id, st_pnt_no,end_pnt_no,segment_Total_pnt_cnt,pnt_diff')

                for row in res:
                    row_count += 1
                    # dict_geom_ret[row_count] = row

                    dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                            'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'], 'pnt_diff': row['pnt_diff']}
                    if row['segment_id'] not in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                                            'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'],
                                                            'pnt_diff': row['pnt_diff'], 'pnt_list': list()}

                    if pSERVER_CREATE_CSV_FLG == True:
                        fsv.write('\n' + str(row['segment_id']) + ','  + str(
                            row['min_pnt_cnt']) + ',' + str(row['max_pnt_cnt']) + ',' + str(row['pnt_cnt']) + ',' + str(row['pnt_diff']))

                    #print row_count

                print "%s - %i Points fit in the given polygon\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv.close()

                pgsql_cursor.close()
                # pgsql_conn.close()
                if row_count == 0:
                    exit(1)

                # Load Point lst
                queryPnts = ""

                if (pProcessTypeFlg == 'ALL'):
                    queryPnts = "select segment_id ,point from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) order by segment_id , point ;" % pWkt

                elif (pProcessTypeFlg == 'PROCESS'):
                    queryPnts = "select segment_id ,point from seis_segment_geom where location_type in ('CMP', 'BIN') and ST_Within(the_geom,ST_GeomFromText('%s',4326)) order by segment_id , point ;" % pWkt

                elif (pProcessTypeFlg == 'FIELD'):
                    queryPnts = "select segment_id ,point from seis_segment_geom where location_type not in ('CMP', 'BIN') and ST_Within(the_geom,ST_GeomFromText('%s',4326)) order by segment_id , point ;" % pWkt

                print queryPnts

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor2 = pPgsqlConn.cursor()
                res = execute_query(pgsql_cursor2, queryPnts)
                res = pgsql_cursor2.fetchall()

                fsv4 = None;

                if pSERVER_CREATE_CSV_FLG == True:
                    #fsv4 = open(SERVER_CREATE_CSV_PATH + 'dict_geom_Pnts_FromDB.csv', 'w+')
                    # fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Pnt_List.csv', 'w+')
                    if pPolygonType == 1:
                        fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Pnt_List_BLK' + str(pBlockNumber) + '.csv', 'w+')
                    else:
                        fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Pnt_List_WKT' + '.csv', 'w+')

                    fsv4.write('segment_id,pnt_no')

                for row in res:
                    # dict_geom_ret[row_count] = row
                    if row[0] in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row[0]]['pnt_list'].append(int(row[1]))

                    if pSERVER_CREATE_CSV_FLG == True:
                        fsv4.write('\n' + row[0] + ',' + str(row[1]))

                pgsql_cursor2.close()

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv4.close()
        else:
            with open( G_DICT_GEOM , 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    if row['segment_id'] not in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                            'min_pnt_cnt': row['min_pnt_cnt'],
                                                            'max_pnt_cnt': row['max_pnt_cnt'],
                                                            'pnt_cnt': row['pnt_cnt'],
                                                            'pnt_diff': row['pnt_diff'],
                                                            'pnt_list': list()}

                        dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                                'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'],
                                                'pnt_diff': row['pnt_diff']}

                del reader

            # with open(G_DICT_GEOM_POINTS , 'r') as f:
            #     reader = csv.DictReader(f)
            #     row_count = 0
            #     for row in reader:
            #         if row['segment_id'] in dict_geom_new_ret.keys():
            #             dict_geom_new_ret[row['segment_id']]['pnt_list'].append(int(row['pnt_no']))
            #
            #     del reader
    except:
        print( "Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)


    return dict_geom_ret , dict_geom_new_ret

def Get_Dict_DictGeom_and_DictGeomNew_Old( pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV  , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG ):

    dict_geom_new_ret = {}
    dict_geom_ret ={}
    row_count = 0;

    try:
        if not pDICT_GEOM_FROM_CSV:
            if pSERVER_TEST_FLG == True:
                # Points in Polygon
                # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                        "where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt

                # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                print query

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                res = execute_query(pgsql_cursor, query)
                res = pgsql_cursor.fetchall()

                dict_geom_ret1 = res

                print "\n%s - Finished: Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                          localtime())
                print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv = open(SERVER_CREATE_CSV_PATH + 'dict_geom_test_FromDB.csv', 'w+')
                    fsv.write('min_pnt_cnt,max_pnt_cnt,segment_id,pnt_cnt,pnt_diff')

                for row in res:
                    row_count += 1
                    # dict_geom_ret[row_count] = row
                    dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                            'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'], 'pnt_diff': row['pnt_diff']}
                    if row['segment_id'] not in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                                            'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'],
                                                            'pnt_diff': row['pnt_diff'], 'pnt_list': list()}

                    if pSERVER_CREATE_CSV_FLG == True:
                        fsv.write('\n' + str(row['segment_id']) + ',' + str(row['pnt_cnt']) + ',' + str(
                            row['min_pnt_cnt']) + ',' + str(row['max_pnt_cnt']) + ',' + str(row['pnt_diff']))

                    print row_count

                print "%s - %i Points fit in the given polygon\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv.close()

                pgsql_cursor.close()
                # pgsql_conn.close()
                if row_count == 0:
                    exit(1)

                # Load Point lst
                queryPnts = "select segment_id ,point from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) order by segment_id , point ;" % pWkt
                print queryPnts

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor2 = pPgsqlConn.cursor()
                res = execute_query(pgsql_cursor2, queryPnts)
                res = pgsql_cursor2.fetchall()

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv4 = open(SERVER_CREATE_CSV_PATH + 'dict_geom_Pnts_FromDB.csv', 'w+')
                    fsv4.write('segment_id,pnt_no')

                for row in res:
                    # dict_geom_ret[row_count] = row
                    if row[0] in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row[0]]['pnt_list'].append(int(row[1]))

                    if pSERVER_CREATE_CSV_FLG == True:
                        fsv4.write('\n' + row[0] + ',' + str(row[1]))

                pgsql_cursor2.close()

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv4.close()
        else:
            with open( G_DICT_GEOM , 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    if row['segment_id'] not in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                            'min_pnt_cnt': row['min_pnt_cnt'],
                                                            'max_pnt_cnt': row['max_pnt_cnt'],
                                                            'pnt_cnt': row['pnt_cnt'],
                                                            'pnt_diff': row['pnt_diff'],
                                                            'pnt_list': list()}

                        dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                                'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'],
                                                'pnt_diff': row['pnt_diff']}

                del reader

            with open(G_DICT_GEOM_POINTS , 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    if row['segment_id'] in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']]['pnt_list'].append(int(row['pnt_no']))

                del reader
    except:
        print( "Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_geom_ret , dict_geom_new_ret

def Get_Dict_DictGeom_and_DictGeomNewList( pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV  , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG , pBlockNumber , pPolygonType):

    dict_geom_ret = []
    row_count = 0;

    try:
        if not pDICT_GEOM_FROM_CSV:
            if pSERVER_TEST_FLG == True:
                # Points in Polygon
                # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                if pPolygonType == 1:
                    # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                    #         "where ST_Within(the_geom, (select the_geom from concessions where license1 like 'BLOCK %s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                    query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                            "where ST_Within(the_geom, (select geom from concessions where blocknumber = '%s' limit 1 ) ) group by segment_id order by segment_id" % pBlockNumber
                else:
                    # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                    #     "where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt
                    query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                        "where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt

                # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                print query

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                res = execute_query(pgsql_cursor, query)
                res = pgsql_cursor.fetchall()

                dict_geom_ret = res

                print "\n%s - Finished: Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                          localtime())
                print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

                if len(res) == 0:
                    exit(1)

    except:
        print( "Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)


    return dict_geom_ret


def Get_SegmentPointList( pWkt, pPgsqlConn, pSegId , pBlockNUmber  , pPolygonType):

    ret_pntList = []

    try:
        # Load Point lst
        if pPolygonType == 1 :
            # queryPnts = "select point from seis_segment_geom where segment_id = '{0}' and ST_Within(the_geom,(select the_geom from concessions where license1 like 'BLOCK {1}' limit 1 )) order by point ;".format(
            #     pSegId, pBlockNUmber )
            queryPnts = "select point from seis_segment_geom where segment_id = '{0}' and ST_Within(the_geom,(select geom from concessions where blocknumber = '{1}' limit 1 )) order by point ;".format(
                pSegId, pBlockNUmber )
        else:
            queryPnts = "select point from seis_segment_geom where segment_id = '{0}' and ST_Within(the_geom,ST_GeomFromText('{1}',4326)) order by point ;".format(
                pSegId, pWkt)

        print queryPnts

        print "\n%s - Request spatial database for Reading points for segement....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                        localtime())
        pgsql_cursor2 = pPgsqlConn.cursor()
        res = execute_query(pgsql_cursor2, queryPnts)
        res = pgsql_cursor2.fetchall()

        for row in res:
            # dict_geom_ret[row_count] = row
            #dict_geom_new_ret[row[0]]['pnt_list'].append(int(row[1]))
            ret_pntList.append(int(row[0]))

        pgsql_cursor2.close()

        print "\n%s - Finished : Request spatial database for Reading points for segement....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())

    except:
        print( "Error in Reading points for segement from postGis Database.")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return ret_pntList

# Added path , block number, server path variable pSERVER_CREATE_CSV_PATH on 18 Nov 2018
def Get_Dict_Dict_ppdm( pMssql_conn, pSegment_id_set, pDICT_PPDM_FROM_CSV,pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG , pSERVER_CREATE_CSV_PATH ,
                                               pPolygonType ,
                                               pBlockNumber ):

    dict_ppdm_ret = {}
    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0


    #pSERVER_TEST_FLG = True  # For Server query fetch  test on with local run  oterwise comment it.

    try:
        set_size = 10000
        total_queryCouunt = len(pSegment_id_set) / set_size

        if not pDICT_PPDM_FROM_CSV:
            for seg in pSegment_id_set:
                seg_nb += 1
                seg_query += 1
                segment_id_set_string1 += "\'%s\'," % seg

                if pSERVER_TEST_FLG == True:
                    if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                        segment_id_set_string1 = segment_id_set_string1[:-1]
                        query_count += 1

                        # print segment_id_set_string1
                        query = "select a.geom_segment_id , a.seis_file_id, a.section_name,a.section_id,a.dataset_id," \
                                " b.processing_name , b.survey_name,b.proc_set_type,b.store_id,b.location_reference,b.original_file_name,b.header_format,b.point_type,b.product_type ,b.remark," \
                                " min(a.point_start) min_point_start,max(a.point_end) max_point_end,min(a.ffid_start) min_ffid_start,max(a.ffid_end) max_ffid_end," \
                                " min(a.sp_start) min_sp_start,max(a.sp_end) max_sp_end " \
                                " from  fuseim_seis_match a left join fuseim_seis_file b on a.seis_file_id = b.seis_file_id " \
                                " where a.segment_status != 5 " \
                                " and a.geom_segment_id in (%s) " \
                                " group by a.geom_segment_id, a.seis_file_id , a.section_name,a.section_id ,a.dataset_id, " \
                                " b.processing_name , b.survey_name,b.proc_set_type,b.store_id,b.location_reference,b.original_file_name,b.header_format, " \
                                " b.point_type,b.product_type , b.remark " \
                                " order by a.geom_segment_id , min_point_start " % segment_id_set_string1

                        print query

                        print ("\n " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) +" - Request PPDM #" + str( query_count ) + " / " + str( total_queryCouunt ) + "  - "+ str(seg_query ) + " segments queried" )

                        segment_id_set_string1 = ""
                        seg_query = 0

                        mssql_cursor = pMssql_conn.cursor(as_dict=True)
                        mssql_cursor.execute(query)

                        if pSERVER_CREATE_CSV_FLG == True:
                            if pPolygonType == 1 :
                                fsv2 = open(pSERVER_CREATE_CSV_PATH + 'dict_ppdm_FromDB_BLK'+ str(pBlockNumber)+ '.csv', 'w+')
                            else:
                                fsv2 = open(pSERVER_CREATE_CSV_PATH + 'dict_ppdm_FromDB_WKT.csv', 'w+')

                            fsv2.write('geom_segment_id,seis_file_id,section_name,section_id,dataset_id,processing_name,survey_name,proc_set_type,store_id,location_reference,original_file_name,header_format,point_type,product_type,remark,min_point_start,max_point_end,min_ffid_start,max_ffid_end,min_sp_start,max_sp_end')

                        for row in mssql_cursor:
                            row_count += 1
                            dict_ppdm_ret[row_count] = row

                            if pSERVER_CREATE_CSV_FLG == True:
                                strWr = ''
                                strWr = str(row['geom_segment_id']) + ',' + str(row['seis_file_id']) + ',' + str(
                                    row['section_name']) + ',' + str(row['section_id']) + ',' + str(
                                    row['dataset_id']) + ',' + str(row['processing_name']) + ','
                                strWr = strWr + str(row['survey_name']) + ',' + str(row['proc_set_type']) + ',' + str(
                                    row['store_id']) + ',' + str(row['location_reference']) + ','
                                strWr = strWr + str(row['original_file_name']) + ',' + str(
                                    row['header_format']) + ',' + str(row['point_type']) + ',' + str(
                                    row['product_type']) + ',' + str(row['remark']) + ','
                                strWr = strWr + str(row['min_point_start']) + ',' + str(row['max_point_end']) + ',' + str(
                                    row['min_ffid_start']) + ','
                                strWr = strWr + str(row['max_ffid_end']) + ',' + str(row['min_sp_start']) + ',' + str(
                                    row['max_sp_end'])

                                fsv2.write('\n' + strWr)

                        print ("\n Finished" + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Get_Dict_Dict_ppdm Request PPDM #" + str(
                            query_count) + " / " + str(total_queryCouunt) + "  - " + str(
                            seg_query) + " segments queried")

                        del mssql_cursor

            if pSERVER_CREATE_CSV_FLG == True:
                fsv2.close()
        else:
            with open(G_DICT_PPDM, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    dict_ppdm_ret[row_count] = row

        print "\n %s - %i Records found in FUSEIM_SEIS_MATCH\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

    except:
        print( "Error in Reading PPDM records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_ppdm_ret

def Get_ListOfPointRangeInsideInPolygon ( pPgSqlConn,  pWktPolyGon , pWktLinestring , pSrid , pSegStPntNo, pSegEndPntNo):
    """
       Returns the list of point ranges which are inside in polygon 
       Args             :
       pWktPolyGon      : (wkt polygon string)
       pWktLinestring   :  wkt string for line
       pSrid            :  SRID
       pSegStPntNo      :  segment start point    eg. 101
       pSegEndPntNo     :  segement End point      eg. 1557

       Returns:
           list : point ranges which are inside in polygon eg, [(200,500) , (701, 950) ]
       """
    lstRangeList = []
    #Get Start and End point of Clipped Lines from Polygon
    strSql = " SELECT distinct (ST_Dump((mytable.the_geom))).path[1] as lineNo," \
             " (ST_Dump(mytable.the_geom)).geom as line_geom ," \
             " ST_AsText(ST_StartPoint((ST_Dump(mytable.the_geom)).geom) ) as stPoint ,"\
             " ST_AsText(ST_EndPoint((ST_Dump(mytable.the_geom)).geom) ) as EndPoint " \
             " from (" \
             " select ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
             " from  ST_GeomFromText('{0}') AS linestring " \
             " CROSS JOIN ST_GeomFromText('{1}') AS polygon "\
             " ) as mytable;"

    strSql=strSql.format(pWktLinestring , pWktPolyGon )
    print strSql
    pgsql_cursor =   pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    res = execute_query(pgsql_cursor, strSql)
    res = pgsql_cursor.fetchall()

    for row in res:

        strStPoint = row["stpoint"]
        strEndPoint = row["endpoint"]

        strsql2 = "SELECT   Mytable.stPointNo , Mytable.endPointNo," \
                  " Mytable.segment_length * ratioPoint1 as FirstLength," \
                  " Mytable.segment_length *  ratioPoint2 as SecondLength," \
                  " Mytable.Totalpoints ," \
                  " Mytable.segment_length/Totalpoints as PointDiff," \
                  " stPointNo + cast (( (Mytable.segment_length * ratioPoint1 ) / (Mytable.segment_length/Totalpoints)) as integer) as FirstClipPntNo," \
                  " stPointNo + cast (( (Mytable.segment_length * ratioPoint2 ) / (Mytable.segment_length/Totalpoints)) as integer) as ScndClipPntNo" \
                  " FROM(" \
                  "      select the_line, the_point1, the_point2, ST_Length(the_line) as segment_length, stPointNo , endPointNo ,( (endPointNo - stPointNo) + 1 ) as Totalpoints ," \
                  "      ST_Line_Locate_Point(foo.the_line, foo.the_point1) as  ratioPoint1, ST_Line_Locate_Point(foo.the_line, foo.the_point2) as ratioPoint2" \
                  "      FROM (" \
                  "           SELECT ST_GeomFromText('{0}') as the_line," \
                  "           ST_GeomFromText('{1}' ) as the_point1," \
                  "           ST_GeomFromText('{2}' ) as the_point2," \
                  "           {3} as stPointNo ," \
                  "           {4} as endPointNo" \
                  "       ) As foo" \
                  " ) Mytable " \

        strsqlM = strsql2.format(pWktLinestring,strStPoint, strEndPoint , pSegStPntNo, pSegEndPntNo )
        print strsqlM
        pgsql_cursor2 = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        res2 = execute_query(pgsql_cursor2, strsqlM)
        res2 = pgsql_cursor2.fetchall()

        for row2 in res2:
            strRange = "(" + str(row2["firstclippntno"]) +"," + str(row2["scndclippntno"]) + ")"
            lstRangeList.append(strRange)

        pgsql_cursor2.close()

    pgsql_cursor.close()
    #Query To Get clipped Point no from line ( pass point number and Total Line

    return lstRangeList

def Get_ListOfPointRangeInsideInPolygonUsingSegID ( pPgSqlConn,  pSegID, pWktPolyGon , pSegStPntNo, pSegEndPntNo, pBlockNumber , pPolygonType, pSrid = '4326' ):
    """
       Returns the list of point ranges which are inside in polygon
       Args             :
       pPgSqlConn       : Postgres Connection
       pSegID           : Segment Id
       pWktPolyGon      : (wkt polygon string)
       pWktLinestring   :  wkt string for line
       pSrid            :  SRID
       pSegStPntNo      :  segment start point    eg. 101
       pSegEndPntNo     :  segement End point      eg. 1557

       Returns:
           list : point ranges which are inside in polygon eg, [(200,500) , (701, 950) ]
       """
    lstRangeList = []
    listPntRangeInsideNew = []      #/Added on 8 Jan 2018

    #Get Start and End point of Clipped Lines from Polygon
    if pPolygonType == 1:
        strSql = " SELECT distinct   (ST_Dump((mytable.the_geom))).path[1] as lineNo, " \
                 " ST_AsText(ST_StartPoint((ST_Dump(mytable.the_geom)).geom) ) as stPoint ," \
                 " ST_AsText(ST_EndPoint((ST_Dump(mytable.the_geom)).geom) ) as EndPoint " \
                 " from ( " \
                 "      select 1 as point , ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "      from  (  " \
                 "		        select ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "		        from ( " \
                 "					    select point, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id in ('{0}')  order by point " \
                 "				     ) as r  " \
                 "		      ) as t " \
                 "        CROSS JOIN " \
                 "		 ( select geom as polygon from concessions where blocknumber = '{1}' limit 1 ) as r1" \
                 "     ) as mytable;"

                # "		 ( select the_geom as polygon from concessions where license1 like 'BLOCK {1}' limit 1 ) as r1" \

        strSql = strSql.format(pSegID, pBlockNumber)

    else:
        strSql = " SELECT distinct   (ST_Dump((mytable.the_geom))).path[1] as lineNo, " \
                 " ST_AsText(ST_StartPoint((ST_Dump(mytable.the_geom)).geom) ) as stPoint ," \
                 " ST_AsText(ST_EndPoint((ST_Dump(mytable.the_geom)).geom) ) as EndPoint " \
                 " from ( " \
                 "      select 1 as point , ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "      from  (  " \
                 "		        select ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "		        from ( " \
                 "					    select point, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id in ('{0}')  order by point " \
                 "				     ) as r  " \
                 "		      ) as t " \
                 "        CROSS JOIN " \
                 "		 ST_GeomFromText('{1}',{2}) AS polygon" \
                 "     ) as mytable;"

        strSql=strSql.format( pSegID, pWktPolyGon , pSrid)

    print strSql
    pgsql_cursor =   pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    res = execute_query(pgsql_cursor, strSql)
    res = pgsql_cursor.fetchall()

    for row in res:

        strClipStPointXY = row["stpoint"]
        strClipEndPointXY = row["endpoint"]

        strsql2 = "SELECT   Mytable.stPointNo , Mytable.endPointNo, " \
                  " Mytable.segment_length * ratioPoint1 as FirstLength, " \
                  " Mytable.segment_length *  ratioPoint2 as SecondLength, " \
                  " Mytable.Totalpoints , " \
                  " Mytable.segment_length/Totalpoints as PointDiff, " \
                  " stPointNo + cast (( (Mytable.segment_length * ratioPoint1 ) / (Mytable.segment_length/Totalpoints)) as integer) as FirstClipPntNo, " \
                  " stPointNo + cast (( (Mytable.segment_length * ratioPoint2 ) / (Mytable.segment_length/Totalpoints)) as integer) as ScndClipPntNo " \
                  " FROM( " \
                  "     select the_line, the_point1, the_point2, ST_Length(the_line) as segment_length, stPointNo , endPointNo ,( (endPointNo - stPointNo) + 1 ) as Totalpoints ,"\
                  "      ST_Line_Locate_Point(foo.the_line, foo.the_point1) as  ratioPoint1, ST_Line_Locate_Point(foo.the_line, foo.the_point2) as ratioPoint2  " \
                  "      FROM ( " \
                  "          SELECT " \
                  "              (select ST_MakeLine(r.the_geom ORDER BY r.point) as the_line " \
                  "              from ( " \
                  "                  select point, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id in ('{0}')  order by point " \
                  "                  ) as r " \
                  "              ), " \
                  "          ST_GeomFromText('{1}',{2} ) as the_point1, " \
                  "          ST_GeomFromText('{3}',{4} ) as the_point2, " \
                  "          {5} as stPointNo , " \
                  "          {6} as endPointNo  " \
                  "          ) As foo " \
                  " ) Mytable  "

        strsqlM = strsql2.format(pSegID,strClipStPointXY, pSrid, strClipEndPointXY ,pSrid, pSegStPntNo, pSegEndPntNo )
        print strsqlM
        pgsql_cursor2 = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        res2 = execute_query(pgsql_cursor2, strsqlM)
        res2 = pgsql_cursor2.fetchall()

        for row2 in res2:
            if (int(pSegEndPntNo)) < int(row2["scndclippntno"]):
                strRange = "(" + str(row2["firstclippntno"]) + "," + str(pSegEndPntNo) + ")"
                d = TempLateClasses.clsRngDetais(str(pSegID), int(row2["firstclippntno"]), int(pSegEndPntNo),
                                                     int(pSegEndPntNo) - int(row2["firstclippntno"]),
                                                     int(pSegEndPntNo) - int(pSegStPntNo) )
            else:
                strRange = "(" + str(row2["firstclippntno"]) +"," + str(row2["scndclippntno"]) + ")"
                d = TempLateClasses.clsRngDetais(str(pSegID), int(row2["firstclippntno"]), int(row2["scndclippntno"]),
                                                     int(row2["scndclippntno"]) - int(row2["firstclippntno"]),
                                                     int(pSegEndPntNo) - int(pSegStPntNo) )

            lstRangeList.append(strRange)
            listPntRangeInsideNew.append(d)




        pgsql_cursor2.close()

    pgsql_cursor.close()
    #Query To Get clipped Point no from line ( pass point number and Total Line

    return lstRangeList , listPntRangeInsideNew

def Geometry_Test_Function(pPgsqlConn ):
    pSegId = 'OXY88HFT1-Source-OXY88HFT1-00010-1'
    pWktPolyGon = "POLYGON((56.01746893310849 23.45318917067979,56.00991583252255 23.547013665910185,56.099373779599 23.52016097975923,56.07995367431943 23.593586212702142,56.153424743655364 23.598620093075127,56.19874334717099 23.578483412691742,56.05866766357724 23.46956604025949, 56.01746893310849 23.45318917067979))"
    pWktLinestring ="LINESTRING (55.98073339844052 23.509859836421967,56.027239656357324 23.539458636613013,56.157887939456145 23.61937297702059)"
    pSrid   = 4326
    pSegStPntNo = 101
    pSegEndPntNo = 1557

    #lstRangeLIst= Get_ListOfPointRangeInsideInPolygon(pPgsqlConn, pWktPolyGon, pWktLinestring, pSrid, pSegStPntNo, pSegEndPntNo)
    lstRangeLIst = Get_ListOfPointRangeInsideInPolygonUsingSegID(pPgsqlConn, pSegId, pWktPolyGon,  pSegStPntNo, pSegEndPntNo, pSrid='4326' )

    print lstRangeLIst

def GetMinMaxPointFromGIS(pPgSqlConn, pSegId):
    """
           Returns the wkt format of min max point number from Postgis Database 
           Args             :
           pSegId      : Segment id
           Returns:
               string  : min , max pint number from Piostgis  db for degment id 
        segpntString= ''
     """
    strsql = "select min(point) seg_min_point, max(point) seg_max_point  from seis_segment_geom where segment_id in ('{0}') ".format(pSegId)
    print strsql
    pgsql_cursor1 = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    res = execute_query(pgsql_cursor1, strsql)
    res = pgsql_cursor1.fetchall()

    segMinPnt = ''
    segMaxPnt = ''
    for row in res:
        segMinPnt = row["seg_min_point"]
        segMaxPnt = row["seg_max_point"]
        break

    del pgsql_cursor1

    return segMinPnt, segMaxPnt

def BuildSegemntPointString(pSegId , pPgSqlConn):
    """
       Returns the wkt format of point string of segment
       Args             :
       pSegId      : Segment id

       Returns:
           string  : point string in wkt format
    segpntString= ''
    """
    strsqlM ="select segment_id,point from seis_segment_geom where segment_id in ('OXY88HFT1-Source-OXY88HFT1-00010-1') order by point;".format(pSegId)

    lstRangeList =[]

    print strsqlM
    pgsql_cursor2 = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    res2 = execute_query(pgsql_cursor2, strsqlM)
    res2 = pgsql_cursor2.fetchall()

    for row2 in res2:
        strRange = "(" + str(row2["firstclippntno"]) + "," + str(row2["scndclippntno"]) + ")"
        lstRangeList.append(strRange)

    pgsql_cursor2.close()

    return segpntString;

def WriteDataSheet_old(pMsSqlConn, pDict_Geom, pDict_ppdm ,pOutput_path , pSegment_id_set , pFileName ):
    """
           Returns None
           Args             :
           pMsSqlConn       : Sql server connection object
           pDict_Geom       : Dictionary of segment _id from postgis database  found in polygon shape
           pDict_ppdm       : Dictionary of segment _id from ppdm database  found in polygon shape
           pOutput_path     :  Output path csv
           pSegment_id_set  :  Segment id set
           Returns:
               string  : point string in wkt format
        """
    #Create TempDictionary for data
    dict_dataHolder = {}
    for k, v in pDict_ppdm.iteritems():
        segId = v["geom_segment_id"]
        if segId not in dict_dataHolder.keys():
            dict_dataHolder[segId] = v


    #############################################################
    # Create Work sheet
    if os.path.exists(pOutput_path):
        workbookDataRequest = xlsxwriter.Workbook(pOutput_path+'/Data_Request_' + pFileName + '.xls')

        worksht2DFieldData= workbookDataRequest.add_worksheet(name="2D_FIELD_DATA",worksheet_class=None)
        worksht2DProcessData = workbookDataRequest.add_worksheet(name="2D_PROCESS_DATA", worksheet_class=None)
        worksht3DFieldData = workbookDataRequest.add_worksheet(name="3D_FIELD_DATA", worksheet_class=None)
        worksht3DProcessData = workbookDataRequest.add_worksheet(name="3D_PROCESS_DATA", worksheet_class=None)
        workshtSupportData = workbookDataRequest.add_worksheet(name="SUPPORT_DATA", worksheet_class=None)

        data =('Survey Name','Survey Type','Location Type','Linename','Linealias','Start','End','Trace Data Type','Support Data')
        worksht2DFieldData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linename', 'Start', 'End', 'Trace Data Type','Support Data')
        worksht3DFieldData.write_row('A1', data)

        data = ('Survey Name', 'Survey Type', 'Location Name', 'Linename', 'Linealias', 'Start', 'End', 'Process Product')
        worksht2DProcessData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linealias', 'Start', 'End', 'Process Product Type')
        worksht3DProcessData.write_row('A1', data)

        data = ('Linename', 'Support_Data')
        workshtSupportData.write_row('A1', data)

        #############################################################
        segment_id_set_string1 = ""
        seg_nb = 0
        seg_query = 0
        query_count = 0
        row_count = 0

        old_segmentId = ''

        for seg in pSegment_id_set:
            seg_nb += 1
            seg_query += 1
            segment_id_set_string1 += "\'%s\'," % seg

            print "\n Wait !!, Processing Field data sheet ............Query Count no %d" % query_count

            if seg_nb % 5000 == 0 or seg_nb == len(pSegment_id_set):
                segment_id_set_string1 = segment_id_set_string1[:-1]
                query_count += 1

                fieldquery1 = " select distinct line_alias1 , geom_segment_id1 , geom_segment_name1 as Line_name , c.abstract as abstract1 , c.item_sub_category " \
                        "    from ( " \
                        "       select distinct a.line_alias as line_alias1, b.geom_segment_id  as geom_segment_id1,b.geom_segment_name geom_segment_name1 " \
                        "        from FUSEIM_GEOM_LINE a ,fuseim_seis_match B " \
                        "        WHERE b.geom_segment_name = a.line_name " \
                        "        AND  b.geom_segment_id in( {0}) " \
                        "        ) as MyTable " \
                        "     Left join fuseim_document c " \
                        "     on " \
                        "     c.last_condition like MyTable.geom_segment_name1 " \
                        "     and c.item_sub_category in ('Seismic Field Data', 'Observer Log', 'Report','Seismic Navigtion Data' ) " \
                        " order by geom_segment_id1 "

                fieldquery2 = fieldquery1.format(segment_id_set_string1)

                print fieldquery2
                mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
                mssql_cursor1.execute(fieldquery2)

                rownum2d = 2;
                rownum3d = 2;
                rownumSptData = 2;
                allTraceDataType =''

                for row in mssql_cursor1:
                    v_geom_segmentid = row["geom_segment_id1"]
                    v_linealias = row["line_alias1"]
                    v_line_name = row["Line_name"]
                    v_abstract = str(row["abstract1"])
                    v_item_subcategory = row["item_sub_category"]

                    v_procSetType = dict_dataHolder[v_geom_segmentid]["proc_set_type"]
                    if 'FIELD' in v_procSetType or v_item_subcategory == 'Seismic Field Data':

                        if old_segmentId == v_geom_segmentid:
                            if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                                v_support = "Y"
                                if "2D" in v_survyeType:
                                    worksht2DFieldData.write(rownum2d-2, 8, v_support)
                                elif "3D" in v_survyeType:
                                    worksht3DFieldData.write(rownum3d-2, 7, v_support)

                            elif v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory == 'Seismic Field Data':
                                if v_abstract != None and v_abstract != '':
                                    if 'SEGA' in v_abstract.upper() and  'SEGA' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGA'
                                    elif 'SEGB' in v_abstract.upper() and  'SEGB' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGB'
                                    elif 'SEGC' in v_abstract.upper() and  'SEGC' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGY'
                                    elif 'SEGD' in v_abstract.upper() and  'SEGD' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGD'
                                    elif 'SEGX' in v_abstract.upper() and  'SEGX' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGX'
                                    elif 'SSL' in v_abstract.upper() and  'SSL' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + '/' + 'FIELD SSL'

                                    allTraceDataType = allTraceDataType.lstrip(" / ")

                                    if "2D" in v_survyeType:
                                        worksht2DFieldData.write((rownum2d-2), 7, allTraceDataType)
                                    elif "3D" in v_survyeType:
                                        worksht3DFieldData.write((rownum3d-2), 6, allTraceDataType)



                        else:
                            v_stPnt = ''
                            v_endPnt =''
                            v_survyeName=''
                            v_survyeType=''
                            allTraceDataType=''
                            v_locationType=''
                            v_support=''

                            if v_geom_segmentid in pDict_Geom.keys():
                                v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                                v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                            if v_geom_segmentid in dict_dataHolder.keys():
                                v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                                v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]

                            v_locationType= dict_dataHolder[v_geom_segmentid]["point_type"]

                            if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                                v_support="Y"
                                dataSupport = (v_line_name, v_item_subcategory )
                                workshtSupportData.write_row("A" + str(rownumSptData) , dataSupport)
                                rownumSptData = rownumSptData + 1
                            else:
                                v_support = "N"

                            if 'FIELD' in v_procSetType:
                                allTraceDataType = v_procSetType
                            # Add check for Trace data type like segy , b, d, ...
                            if v_item_subcategory == 'Seismic Field Data':
                                if v_abstract != None and v_abstract != '':
                                    if 'SEGA' in v_abstract.upper() and 'SEGA' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGA'
                                    elif 'SEGB' in v_abstract.upper() and 'SEGB' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGB'
                                    elif 'SEGC' in v_abstract.upper() and 'SEGC' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'
                                    elif 'SEGD' in v_abstract.upper() and 'SEGD' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGD'
                                    elif 'SEGX' in v_abstract.upper() and 'SEGX' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGX'
                                    elif 'SSL' in v_abstract.upper() and 'SSL' not in allTraceDataType:
                                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SSL'

                                    allTraceDataType = allTraceDataType.lstrip(" / ")

                            if 'FIELD' in allTraceDataType:
                                if '2D' in v_survyeType:
                                    data2dField =(v_survyeName,v_survyeType,v_locationType,v_line_name,v_linealias,v_stPnt,v_endPnt,allTraceDataType,v_support )
                                    worksht2DFieldData.write_row("A" + str(rownum2d), data2dField)
                                    rownum2d = rownum2d + 1
                                elif '3D' in v_survyeType:
                                    data3dField = (v_survyeName,v_survyeType,v_locationType,v_line_name,v_stPnt,v_endPnt,allTraceDataType, v_support )
                                    worksht3DFieldData.write_row("A" + str(rownum3d), data3dField)
                                    rownum3d = rownum3d + 1

                    old_segmentId = v_geom_segmentid

                del mssql_cursor1

                print "\n Finished: %s - Processing Field Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

                #### Processing PROCESSDATA ............
                print "\n Wait !!, Processing PROCESSDATA data sheet ............Query Count %d" % (query_count)
                queryProcess2 ="select distinct line_alias1 , geom_segment_id1 , geom_segment_name1 as line_name, c.PROC_SET_TYPE " \
                                " from ( " \
                                "   select distinct a.line_alias as line_alias1, b.geom_segment_id  as geom_segment_id1,b.geom_segment_name geom_segment_name1 " \
                                "    from FUSEIM_GEOM_LINE a ,fuseim_seis_match B " \
                                "   WHERE b.geom_segment_name = a.line_name " \
                                "    AND  b.geom_segment_id in( {0} )" \
                                " ) as MyTable " \
                                " Left join fuseim_seis_section c " \
                                " on " \
                                " c.section_name = MyTable.geom_segment_name1 " \
                                " and c.PROC_SET_TYPE not like 'FIELD%' " \
                                " order by geom_segment_id1 " \

                queryProcess = queryProcess2.format(segment_id_set_string1)

                print queryProcess
                mssql_cursor2 = pMsSqlConn.cursor(as_dict=True)
                mssql_cursor2.execute(queryProcess)
                rownum2d = 2;
                rownum3d = 2;
                vProcessProduct = ''

                for row in mssql_cursor2:
                    v_geom_segmentid = row["geom_segment_id1"]
                    v_linealias = row["line_alias1"]
                    v_line_name = row["line_name"]
                    v_procSetType = str(row["PROC_SET_TYPE"])

                    if 'FIELD' not in str(v_procSetType).upper() and v_procSetType != '' and v_procSetType != None and v_procSetType != "None":
                        if old_segmentId == v_geom_segmentid :
                            if v_procSetType != None and v_procSetType != '' and 'FIELD' not in str(v_procSetType).upper():
                                if v_procSetType not in vProcessProduct:
                                    vProcessProduct = vProcessProduct + ' / ' + v_procSetType

                                    vProcessProduct = vProcessProduct.lstrip(' / ')

                                    if "2D" in v_survyeType:
                                        worksht2DProcessData.write((rownum2d-2), 7,vProcessProduct)
                                    elif "3D" in v_survyeType:
                                        worksht3DProcessData.write((rownum3d-2), 6, vProcessProduct)

                        else:
                            v_stPnt = ''
                            v_endPnt= ''
                            v_survyeName=''
                            v_survyeType=''
                            vProcessProduct=''
                            v_locationType=''

                            if v_geom_segmentid in pDict_Geom.keys():
                                v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                                v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                            if v_geom_segmentid in dict_dataHolder.keys():
                                v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                                v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]
                                #vProcessProduct = dict_dataHolder[v_geom_segmentid]["proc_set_type"]
                                vProcessProduct = ""
                                v_locationType = dict_dataHolder[v_geom_segmentid]["point_type"]

                            if v_procSetType != None and v_procSetType != '' and v_procSetType != "None" and 'FIELD' not in str(v_procSetType).upper() :
                                if v_procSetType not in vProcessProduct:
                                    vProcessProduct = vProcessProduct + ' / ' + v_procSetType

                            vProcessProduct = vProcessProduct.lstrip(' / ')

                            if  '2D' in v_survyeType:
                                data2dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_linealias, v_stPnt, v_endPnt,vProcessProduct)
                                worksht2DProcessData.write_row("A" + str(rownum2d), data2dprocess)
                                rownum2d = rownum2d + 1
                            elif '3D' in v_survyeType:
                                data3dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_stPnt, v_endPnt,vProcessProduct)
                                worksht3DProcessData.write_row("A" + str(rownum3d), data3dprocess)
                                rownum3d = rownum3d + 1

                    old_segmentId = v_geom_segmentid

                del mssql_cursor2

                print "\n Finished: %s - Processing Process Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))
    else:
        print("File path Not Exists >> " , pOutput_path )

# Write data sheet function , further divided in two part create and wrte set
def WriteDataSheet(pMsSqlConn, pDict_Geom, pDict_ppdm ,pOutput_path , pSegment_id_set , pFileName ):
    """
           Returns None
           Args             :
           pMsSqlConn       : Sql server connection object
           pDict_Geom       : Dictionary of segment _id from postgis database  found in polygon shape
           pDict_ppdm       : Dictionary of segment _id from ppdm database  found in polygon shape
           pOutput_path     :  Output path csv
           pSegment_id_set  :  Segment id set
           Returns:
               string  : point string in wkt format
        """
    #Create TempDictionary for data
    dict_dataHolder = {}
    for k, v in pDict_ppdm.iteritems():
        segId = v["geom_segment_id"]
        if segId not in dict_dataHolder.keys():
            dict_dataHolder[segId] = v


    #############################################################
    # Create Work sheet
    if os.path.exists(pOutput_path):
        workbookDataRequest = xlsxwriter.Workbook(pOutput_path+'/Data_Request_' + pFileName + '.xls')

        worksht2DFieldData= workbookDataRequest.add_worksheet(name="2D_FIELD_DATA",worksheet_class=None)
        worksht2DProcessData = workbookDataRequest.add_worksheet(name="2D_PROCESS_DATA", worksheet_class=None)
        worksht3DFieldData = workbookDataRequest.add_worksheet(name="3D_FIELD_DATA", worksheet_class=None)
        worksht3DProcessData = workbookDataRequest.add_worksheet(name="3D_PROCESS_DATA", worksheet_class=None)
        workshtSupportData = workbookDataRequest.add_worksheet(name="SUPPORT_DATA", worksheet_class=None)

        data =('Survey Name','Survey Type','Location Type','Linename','Linealias','Start','End','Trace Data Type','Support Data')
        worksht2DFieldData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linename', 'Start', 'End', 'Trace Data Type','Support Data')
        worksht3DFieldData.write_row('A1', data)

        data = ('Survey Name', 'Survey Type', 'Location Name', 'Linename', 'Linealias', 'Start', 'End', 'Process Product')
        worksht2DProcessData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linealias', 'Start', 'End', 'Process Product Type')
        worksht3DProcessData.write_row('A1', data)

        data = ('Linename', 'Support_Data')
        workshtSupportData.write_row('A1', data)

        dict_support_list = {}
        #############################################################
        segment_id_set_string1 = ""
        seg_nb = 0
        seg_query = 0
        query_count = 0
        row_count = 0

        old_segmentId = ''

        for seg in pSegment_id_set:
            seg_nb += 1
            seg_query += 1
            segment_id_set_string1 += "\'%s\'," % seg

            rownum2d = 2;
            rownum3d = 2;
            rownumSptData = 2;
            rownum2dProcess = 2;
            rownum3dProcess = 2;

            print "\n Wait !!, Processing Field data sheet ............Query Count no %d" % query_count

            if seg_nb % 5000 == 0 or seg_nb == len(pSegment_id_set):
                segment_id_set_string1 = segment_id_set_string1[:-1]
                query_count += 1

                ### Processing Support Data
                print "\n Wait !!, Processing PROCESSDATA data sheet ............Query Count %d" % (query_count)
                #queryProcess3 = " select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type, c.abstract , c.item_sub_category from " \
                queryProcess3 = " select distinct MyTable2.line_name , c.item_sub_category from " \
                                        " ( " \
                                "select line_name ,line_alias,  geom_segment_id, d.proc_set_type from " \
                                "    ( " \
                                "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                                "       from FUSEIM_GEOM_LINE a ,fuseim_seis_match B         WHERE b.geom_segment_name = a.line_name " \
                                "        AND  b.geom_segment_id in( {0} ) " \
                                "    ) Mytable " \
                                "    join fuseim_seis_section d " \
                                "    on d.section_name = MyTable.Line_name " \
                                "     and d.proc_set_type like  'FIELD%' " \
                                " )MyTable2 " \
                                " Left join fuseim_document c " \
                                " on c.last_condition like MyTable2.line_name " \
                                " and item_sub_category in ('Observer Log', 'Report','Seismic Navigtion Data' ) " \
                                " order by MyTable2.line_name " \

                queryProcess_3f = queryProcess3.format(segment_id_set_string1)

                print queryProcess_3f
                mssql_cursor3 = pMsSqlConn.cursor(as_dict=True)
                mssql_cursor3.execute(queryProcess_3f)
                vProcessProduct = ''

                for row in mssql_cursor3:
                    # v_geom_segmentid = row["geom_segment_id"]
                    # v_linealias = row["line_alias"]
                    v_line_name = row["line_name"]
                    # v_procSetType = str(row["proc_set_type"])
                    v_item_subcategory = row["item_sub_category"]

                    if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                        v_support = "Y"
                        dataSupport = (v_line_name, v_item_subcategory)
                        workshtSupportData.write_row("A" + str(rownumSptData), dataSupport)
                        rownumSptData = rownumSptData + 1
                        if v_line_name not in dict_support_list.keys():
                            dict_support_list[v_line_name] = {"line_name": v_line_name}
                    else:
                        v_support = "N"

                    # old_segmentId = v_geom_segmentid

                del mssql_cursor3

                ### Processing Field Data
                fieldquery1 = "select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type, MyTable2.point_type ,c.abstract from " \
                            "( " \
                            " select line_name ,line_alias,  geom_segment_id, d.proc_set_type , d.point_type from " \
                            "    ( " \
                            "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                            "        from FUSEIM_GEOM_LINE a ,fuseim_seis_match B WHERE b.geom_segment_name = a.line_name " \
                            "        AND  b.geom_segment_id in( {0})         " \
                            "    ) Mytable " \
                            "    join fuseim_seis_section d " \
                            "    on d.section_name = MyTable.Line_name " \
                            "     and d.proc_set_type like  'FIELD%' " \
                            " )MyTable2 " \
                            " Left join fuseim_document c " \
                            " on c.last_condition like MyTable2.line_name " \
                            " and c.item_sub_category in ('Seismic Field Data' ) " \
                            " order by MyTable2.geom_segment_id , MyTable2.line_alias , MyTable2.point_type " \

                #" and c.item_sub_category in ('Seismic Field Data' ) " \
                fieldquery2 = fieldquery1.format(segment_id_set_string1)

                print fieldquery2
                mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
                mssql_cursor1.execute(fieldquery2)


                allTraceDataType =''
                old_v_linealias= '';
                old_segmentId = ''
                old_v_point_type = ''

                for row in mssql_cursor1:
                    v_geom_segmentid = row["geom_segment_id"]
                    v_linealias = str(row["line_alias"])
                    v_line_name = row["line_name"]
                    v_abstract = str(row["abstract"])
                    # v_item_subcategory = row["item_sub_category"]
                    v_point_type = str(row["point_type"])
                    v_procSetType = row["proc_set_type"]

                    if old_segmentId == v_geom_segmentid and v_linealias == old_v_linealias and v_point_type == old_v_point_type:
                        if v_procSetType != None and v_procSetType != '' and v_procSetType != 'None':
                            if 'SEGA' in v_procSetType.upper() and  'SEGA' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGA'
                            elif 'SEGB' in v_procSetType.upper() and  'SEGB' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGB'
                            elif 'SEGC' in v_procSetType.upper() and  'SEGC' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGC'
                            elif 'SEGD' in v_procSetType.upper() and  'SEGD' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGD'
                            elif 'SEGX' in v_procSetType.upper() and  'SEGX' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGX'
                            elif 'SSL' in v_procSetType.upper() and  'SSL' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SSL'
                            elif 'SEGY' in v_procSetType.upper() and 'SEGY' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                        if v_abstract != None and v_abstract != '' and v_abstract != 'None':
                            if 'SEGA' in v_abstract.upper() and  'SEGA' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGA'
                            elif 'SEGB' in v_abstract.upper() and  'SEGB' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGB'
                            elif 'SEGC' in v_abstract.upper() and  'SEGC' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGY'
                            elif 'SEGD' in v_abstract.upper() and  'SEGD' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGD'
                            elif 'SEGX' in v_abstract.upper() and  'SEGX' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SEGX'
                            elif 'SSL' in v_abstract.upper() and  'SSL' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + '/' + 'FIELD SSL'
                            elif 'SEGY' in v_abstract.upper() and 'SEGY' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                        allTraceDataType = allTraceDataType.lstrip(" / ")

                        if "2D" in v_survyeType:
                            worksht2DFieldData.write((rownum2d-2), 7, allTraceDataType)
                        elif "3D" in v_survyeType:
                            worksht3DFieldData.write((rownum3d-2), 6, allTraceDataType)

                    else:
                        v_stPnt = ''
                        v_endPnt =''
                        v_survyeName=''
                        v_survyeType=''
                        allTraceDataType=''
                        v_locationType=''
                        v_support=''

                        if v_geom_segmentid in pDict_Geom.keys():
                            v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                            v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                        if v_geom_segmentid in dict_dataHolder.keys():
                            v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                            v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]

                        v_locationType= v_point_type

                        # if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                        #     v_support="Y"
                        #     dataSupport = (v_line_name, v_item_subcategory )
                        #     workshtSupportData.write_row("A" + str(rownumSptData) , dataSupport)
                        #     rownumSptData = rownumSptData + 1
                        # else:
                        #     v_support = "N"

                        if 'FIELD' in v_procSetType:
                            allTraceDataType = v_procSetType
                        # Add check for Trace data type like segy , b, d, ...
                        if v_abstract != None and v_abstract != '':
                            if 'SEGA' in v_abstract.upper() and 'SEGA' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGA'
                            elif 'SEGB' in v_abstract.upper() and 'SEGB' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGB'
                            elif 'SEGC' in v_abstract.upper() and 'SEGC' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGC'
                            elif 'SEGD' in v_abstract.upper() and 'SEGD' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGD'
                            elif 'SEGX' in v_abstract.upper() and 'SEGX' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGX'
                            elif 'SSL' in v_abstract.upper() and 'SSL' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SSL'
                            elif 'SEGY' in v_abstract.upper() and 'SEGY' not in allTraceDataType:
                                allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                        allTraceDataType = allTraceDataType.lstrip(" / ")

                        if 'FIELD' in allTraceDataType.upper():
                            if v_line_name in dict_support_list.keys():
                                v_support='Y'

                            if '2D' in v_survyeType:
                                data2dField =(v_survyeName,v_survyeType,v_locationType,v_line_name,v_linealias,v_stPnt,v_endPnt,allTraceDataType,v_support )
                                worksht2DFieldData.write_row("A" + str(rownum2d), data2dField)
                                rownum2d = rownum2d + 1
                            elif '3D' in v_survyeType:
                                data3dField = (v_survyeName,v_survyeType,v_locationType,v_line_name,v_stPnt,v_endPnt,allTraceDataType, v_support )
                                worksht3DFieldData.write_row("A" + str(rownum3d), data3dField)
                                rownum3d = rownum3d + 1

                    old_segmentId = v_geom_segmentid
                    old_v_linealias = v_linealias
                    old_v_point_type = v_point_type

                del mssql_cursor1

                print "\n Finished: %s - Processing Field Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))


                #### Processing PROCESSDATA ............
                print "\n Wait !!, Processing PROCESSDATA data sheet ............Query Count %d" % (query_count)
                queryProcess2 ="select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type ,  MyTable2.point_type from " \
                                " ( " \
                                " select line_name ,line_alias,  geom_segment_id, d.proc_set_type , d.point_type from " \
                                "    ( " \
                                "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                                "        from FUSEIM_GEOM_LINE a ,fuseim_seis_match B         WHERE b.geom_segment_name = a.line_name  " \
                                "        AND  b.geom_segment_id in({0}) " \
                                "     ) Mytable " \
                                "    join fuseim_seis_section d " \
                                "    on d.section_name = MyTable.Line_name " \
                                "     and d.proc_set_type not like  'FIELD%' " \
                                " )MyTable2 " \
                                " Left join fuseim_document c " \
                                " on c.last_condition like MyTable2.line_name " \
                                " order by MyTable2.geom_segment_id , MyTable2.line_alias,  MyTable2.point_type"

                queryProcess = queryProcess2.format(segment_id_set_string1)

                print queryProcess
                mssql_cursor2 = pMsSqlConn.cursor(as_dict=True)
                mssql_cursor2.execute(queryProcess)
                vProcessProduct = ''
                old_segmentId=''
                old_v_linealias=''
                old_v_point_type = ''
                lstProc_Set_type = []

                for row in mssql_cursor2:
                    v_geom_segmentid = row["geom_segment_id"]
                    v_linealias = row["line_alias"]
                    v_line_name = row["line_name"]
                    v_procSetType = str(row["proc_set_type"])
                    v_point_type = str(row["point_type"])

                    if 'FIELD' not in str(v_procSetType).upper() and v_procSetType != '' and v_procSetType != None and v_procSetType != "None":
                        if old_segmentId == v_geom_segmentid and v_linealias == old_v_linealias and v_point_type == old_v_point_type:
                            if v_procSetType != None and v_procSetType != '' and 'FIELD' not in str(v_procSetType).upper():
                                # if v_procSetType not in vProcessProduct:
                                if v_procSetType  not in lstProc_Set_type:
                                    vProcessProduct = vProcessProduct + ' / ' + v_procSetType
                                    lstProc_Set_type.append(v_procSetType)

                                    vProcessProduct = vProcessProduct.lstrip(' / ')
                                    if "2D" in v_survyeType:
                                        worksht2DProcessData.write((rownum2dProcess-2), 7,vProcessProduct)
                                    elif "3D" in v_survyeType:
                                        worksht3DProcessData.write((rownum3dProcess-2), 6, vProcessProduct)

                        else:
                            v_stPnt = ''
                            v_endPnt= ''
                            v_survyeName=''
                            v_survyeType=''
                            vProcessProduct=''
                            v_locationType=''

                            lstProc_Set_type = []
                            if v_geom_segmentid in pDict_Geom.keys():
                                v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                                v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                            if v_geom_segmentid in dict_dataHolder.keys():
                                v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                                v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]
                                #vProcessProduct = dict_dataHolder[v_geom_segmentid]["proc_set_type"]
                                vProcessProduct = ""
                                v_locationType = v_point_type

                            if v_procSetType != None and v_procSetType != '' and v_procSetType != "None" and 'FIELD' not in str(v_procSetType).upper() :
                                vProcessProduct = v_procSetType
                                lstProc_Set_type.append(v_procSetType)
                            #     if v_procSetType not in vProcessProduct:
                            #         vProcessProduct = vProcessProduct + ' / ' + v_procSetType
                            #vProcessProduct = vProcessProduct.lstrip(' / ')

                            if  '2D' in v_survyeType:
                                data2dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_linealias, v_stPnt, v_endPnt,vProcessProduct)
                                worksht2DProcessData.write_row("A" + str(rownum2dProcess), data2dprocess)
                                rownum2dProcess = rownum2dProcess + 1
                            elif '3D' in v_survyeType:
                                data3dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_stPnt, v_endPnt,vProcessProduct)
                                worksht3DProcessData.write_row("A" + str(rownum3dProcess), data3dprocess)
                                rownum3dProcess = rownum3dProcess + 1

                    old_segmentId = v_geom_segmentid
                    old_v_linealias = v_linealias
                    old_v_point_type = v_point_type

                del mssql_cursor2

                print "\n Finished: %s - Processing Process Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))
    else:
        print("File path Not Exists >> " , pOutput_path )

def CreateDataSheet( pOutput_path , pFileName ):
    """
           Returns None
           Args             :
           pMsSqlConn       : Sql server connection object
           pDict_Geom       : Dictionary of segment _id from postgis database  found in polygon shape
           pDict_ppdm       : Dictionary of segment _id from ppdm database  found in polygon shape
           pOutput_path     :  Output path csv
           pSegment_id_set  :  Segment id set
           Returns:
               string  : point string in wkt format
        """
    #############################################################
    # Create Work sheet
    if os.path.exists(pOutput_path):
        workbookDataRequest = xlsxwriter.Workbook(pOutput_path+'/Data_Request_' + pFileName + '.xls')

        worksht2DFieldData= workbookDataRequest.add_worksheet(name="2D_FIELD_DATA",worksheet_class=None)
        worksht2DProcessData = workbookDataRequest.add_worksheet(name="2D_PROCESS_DATA", worksheet_class=None)
        worksht3DFieldData = workbookDataRequest.add_worksheet(name="3D_FIELD_DATA", worksheet_class=None)
        worksht3DProcessData = workbookDataRequest.add_worksheet(name="3D_PROCESS_DATA", worksheet_class=None)
        workshtSupportData = workbookDataRequest.add_worksheet(name="SUPPORT_DATA", worksheet_class=None)

        data =('Survey Name','Survey Type','Location Type','Linename','Linealias','Start','End','Trace Data Type','Support Data')
        worksht2DFieldData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linename', 'Start', 'End', 'Trace Data Type','Support Data')
        worksht3DFieldData.write_row('A1', data)

        data = ('Survey Name', 'Survey Type', 'Location Name', 'Linename', 'Linealias', 'Start', 'End', 'Process Product')
        worksht2DProcessData.write_row('A1', data)
        data = ('Survey Name', 'Survey Type', 'Location Type', 'Linealias', 'Start', 'End', 'Process Product Type')
        worksht3DProcessData.write_row('A1', data)

        data = ('Linename', 'Support_Data')
        workshtSupportData.write_row('A1', data)

        return workbookDataRequest
    else:
        print("File path Not Exists >> " , pOutput_path )

        return None

def WriteDataSheet_Set(pMsSqlConn, pDict_Geom, pDict_ppdm , pSegment_id_set , pWorkbookDataRequest ,pRowNum  ):
    """
           Returns None
           Args             :
           pMsSqlConn       : Sql server connection object
           pDict_Geom       : Dictionary of segment _id from postgis database  found in polygon shape
           pDict_ppdm       : Dictionary of segment _id from ppdm database  found in polygon shape
           pOutput_path     :  Output path csv
           pSegment_id_set  :  Segment id set
           Returns:
               string  : point string in wkt format
        """
    #Create TempDictionary for data
    dict_dataHolder = {}
    for k, v in pDict_ppdm.iteritems():
        segId = v["geom_segment_id"]
        if segId not in dict_dataHolder.keys():
            dict_dataHolder[segId] = v


    #############################################################
    # Create Work sheet
    if (pWorkbookDataRequest != None):

        worksht2DFieldData= pWorkbookDataRequest.get_worksheet_by_name("2D_FIELD_DATA")
        worksht2DProcessData = pWorkbookDataRequest.get_worksheet_by_name("2D_PROCESS_DATA")
        worksht3DFieldData = pWorkbookDataRequest.get_worksheet_by_name("3D_FIELD_DATA")
        worksht3DProcessData = pWorkbookDataRequest.get_worksheet_by_name("3D_PROCESS_DATA")
        workshtSupportData = pWorkbookDataRequest.get_worksheet_by_name("SUPPORT_DATA")

        dict_support_list = {}
        #############################################################
        segment_id_set_string1 = ""
        seg_nb = 0
        seg_query = 0
        query_count = 0
        row_count = 0

        old_segmentId = ''

        for seg in pSegment_id_set:
            segment_id_set_string1 += "\'%s\'," % seg

        segment_id_set_string1 = segment_id_set_string1[:-1]

        if pRowNum == "2" :
            rownum2d = 2;
            rownum3d = 2;
            rownumSptData = 2;
            rownum2dProcess = 2;
            rownum3dProcess = 2;
        else:
            rownum2dStr,rownum3dStr,rownumSptDataStr,rownum2dProcessStr,rownum3dProcessStr = pRowNum.split(",")
            rownum2d = int(rownum2dStr)
            rownum3d = int(rownum3dStr)
            rownumSptData = int(rownumSptDataStr)
            rownum2dProcess = int(rownum2dProcessStr)
            rownum3dProcess = int(rownum3dProcessStr)


        print "\n Wait !!, Processing Field data sheet ............Query Count no %d" % query_count

        ### Processing Support Data
        print "\n Wait !!, Processing PROCESSDATA data sheet ............Query Count %d" % (query_count)
        #queryProcess3 = " select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type, c.abstract , c.item_sub_category from " \
        queryProcess3 = " select distinct MyTable2.line_name , c.item_sub_category from " \
                                " ( " \
                        "select line_name ,line_alias,  geom_segment_id, d.proc_set_type from " \
                        "    ( " \
                        "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                        "       from FUSEIM_GEOM_LINE a ,fuseim_seis_match B         WHERE b.geom_segment_name = a.line_name " \
                        "        AND  b.geom_segment_id in( {0} ) " \
                        "    ) Mytable " \
                        "    join fuseim_seis_section d " \
                        "    on d.section_name = MyTable.Line_name " \
                        "     and d.proc_set_type like  'FIELD%' " \
                        " )MyTable2 " \
                        " Left join fuseim_document c " \
                        " on c.last_condition like MyTable2.line_name " \
                        " and item_sub_category in ('Observer Log', 'Report','Seismic Navigtion Data' ) " \
                        " order by MyTable2.line_name " \

        queryProcess_3f = queryProcess3.format(segment_id_set_string1)

        print queryProcess_3f
        mssql_cursor3 = pMsSqlConn.cursor(as_dict=True)
        mssql_cursor3.execute(queryProcess_3f)
        vProcessProduct = ''

        for row in mssql_cursor3:
            # v_geom_segmentid = row["geom_segment_id"]
            # v_linealias = row["line_alias"]
            v_line_name = row["line_name"]
            # v_procSetType = str(row["proc_set_type"])
            v_item_subcategory = row["item_sub_category"]

            if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                v_support = "Y"
                dataSupport = (v_line_name, v_item_subcategory)
                workshtSupportData.write_row("A" + str(rownumSptData), dataSupport)
                rownumSptData = rownumSptData + 1
                if v_line_name not in dict_support_list.keys():
                    dict_support_list[v_line_name] = {"line_name": v_line_name}
            else:
                v_support = "N"

            # old_segmentId = v_geom_segmentid

        del mssql_cursor3

        ### Processing Field Data
        fieldquery1 = "select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type, MyTable2.point_type ,c.abstract from " \
                    "( " \
                    " select line_name ,line_alias,  geom_segment_id, d.proc_set_type , d.point_type from " \
                    "    ( " \
                    "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                    "        from FUSEIM_GEOM_LINE a ,fuseim_seis_match B WHERE b.geom_segment_name = a.line_name " \
                    "        AND  b.geom_segment_id in( {0})         " \
                    "    ) Mytable " \
                    "    join fuseim_seis_section d " \
                    "    on d.section_name = MyTable.Line_name " \
                    "     and d.proc_set_type like  'FIELD%' " \
                    " )MyTable2 " \
                    " Left join fuseim_document c " \
                    " on c.last_condition like MyTable2.line_name " \
                    " and c.item_sub_category in ('Seismic Field Data' ) " \
                    " order by MyTable2.geom_segment_id , MyTable2.line_alias , MyTable2.point_type " \

        #" and c.item_sub_category in ('Seismic Field Data' ) " \
        fieldquery2 = fieldquery1.format(segment_id_set_string1)

        print fieldquery2
        mssql_cursor1 = pMsSqlConn.cursor(as_dict=True)
        mssql_cursor1.execute(fieldquery2)


        allTraceDataType =''
        old_v_linealias= '';
        old_segmentId = ''
        old_v_point_type = ''

        for row in mssql_cursor1:
            v_geom_segmentid = row["geom_segment_id"]
            v_linealias = str(row["line_alias"])
            v_line_name = row["line_name"]
            v_abstract = str(row["abstract"])
            # v_item_subcategory = row["item_sub_category"]
            v_point_type = str(row["point_type"])
            v_procSetType = row["proc_set_type"]

            if old_segmentId == v_geom_segmentid and v_linealias == old_v_linealias and v_point_type == old_v_point_type:
                if v_procSetType != None and v_procSetType != '' and v_procSetType != 'None':
                    if 'SEGA' in v_procSetType.upper() and  'SEGA' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGA'
                    elif 'SEGB' in v_procSetType.upper() and  'SEGB' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGB'
                    elif 'SEGC' in v_procSetType.upper() and  'SEGC' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGC'
                    elif 'SEGD' in v_procSetType.upper() and  'SEGD' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGD'
                    elif 'SEGX' in v_procSetType.upper() and  'SEGX' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGX'
                    elif 'SSL' in v_procSetType.upper() and  'SSL' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SSL'
                    elif 'SEGY' in v_procSetType.upper() and 'SEGY' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                if v_abstract != None and v_abstract != '' and v_abstract != 'None':
                    if 'SEGA' in v_abstract.upper() and  'SEGA' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGA'
                    elif 'SEGB' in v_abstract.upper() and  'SEGB' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGB'
                    elif 'SEGC' in v_abstract.upper() and  'SEGC' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGY'
                    elif 'SEGD' in v_abstract.upper() and  'SEGD' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGD'
                    elif 'SEGX' in v_abstract.upper() and  'SEGX' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SEGX'
                    elif 'SSL' in v_abstract.upper() and  'SSL' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + '/' + 'FIELD SSL'
                    elif 'SEGY' in v_abstract.upper() and 'SEGY' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                allTraceDataType = allTraceDataType.lstrip(" / ")

                if "2D" in v_survyeType:
                    worksht2DFieldData.write((rownum2d-2), 7, allTraceDataType)
                elif "3D" in v_survyeType:
                    worksht3DFieldData.write((rownum3d-2), 6, allTraceDataType)

            else:
                v_stPnt = ''
                v_endPnt =''
                v_survyeName=''
                v_survyeType=''
                allTraceDataType=''
                v_locationType=''
                v_support=''

                if v_geom_segmentid in pDict_Geom.keys():
                    v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                    v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                if v_geom_segmentid in dict_dataHolder.keys():
                    v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                    v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]

                v_locationType= v_point_type

                # if v_item_subcategory != None and v_item_subcategory != '' and v_item_subcategory != 'Seismic Field Data':
                #     v_support="Y"
                #     dataSupport = (v_line_name, v_item_subcategory )
                #     workshtSupportData.write_row("A" + str(rownumSptData) , dataSupport)
                #     rownumSptData = rownumSptData + 1
                # else:
                #     v_support = "N"

                if 'FIELD' in v_procSetType:
                    allTraceDataType = v_procSetType
                # Add check for Trace data type like segy , b, d, ...
                if v_abstract != None and v_abstract != '':
                    if 'SEGA' in v_abstract.upper() and 'SEGA' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGA'
                    elif 'SEGB' in v_abstract.upper() and 'SEGB' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGB'
                    elif 'SEGC' in v_abstract.upper() and 'SEGC' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGC'
                    elif 'SEGD' in v_abstract.upper() and 'SEGD' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGD'
                    elif 'SEGX' in v_abstract.upper() and 'SEGX' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGX'
                    elif 'SSL' in v_abstract.upper() and 'SSL' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SSL'
                    elif 'SEGY' in v_abstract.upper() and 'SEGY' not in allTraceDataType:
                        allTraceDataType = allTraceDataType + ' / ' + 'FIELD SEGY'

                allTraceDataType = allTraceDataType.lstrip(" / ")

                if 'FIELD' in allTraceDataType.upper():
                    if v_line_name in dict_support_list.keys():
                        v_support='Y'

                    if '2D' in v_survyeType:
                        data2dField =(v_survyeName,v_survyeType,v_locationType,v_line_name,v_linealias,v_stPnt,v_endPnt,allTraceDataType,v_support )
                        worksht2DFieldData.write_row("A" + str(rownum2d), data2dField)
                        rownum2d = rownum2d + 1
                    elif '3D' in v_survyeType:
                        data3dField = (v_survyeName,v_survyeType,v_locationType,v_line_name,v_stPnt,v_endPnt,allTraceDataType, v_support )
                        worksht3DFieldData.write_row("A" + str(rownum3d), data3dField)
                        rownum3d = rownum3d + 1

            old_segmentId = v_geom_segmentid
            old_v_linealias = v_linealias
            old_v_point_type = v_point_type

        del mssql_cursor1

        print "\n Finished: %s - Processing Field Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))


        #### Processing PROCESSDATA ............
        print "\n Wait !!, Processing PROCESSDATA data sheet ............Query Count %d" % (query_count)
        queryProcess2 ="select distinct MyTable2.line_name ,MyTable2.line_alias,  MyTable2.geom_segment_id, MyTable2.proc_set_type ,  MyTable2.point_type from " \
                        " ( " \
                        " select line_name ,line_alias,  geom_segment_id, d.proc_set_type , d.point_type from " \
                        "    ( " \
                        "        select distinct a.line_alias as line_alias, b.geom_segment_id  as geom_segment_id, b.geom_segment_name as line_name " \
                        "        from FUSEIM_GEOM_LINE a ,fuseim_seis_match B         WHERE b.geom_segment_name = a.line_name  " \
                        "        AND  b.geom_segment_id in({0}) " \
                        "     ) Mytable " \
                        "    join fuseim_seis_section d " \
                        "    on d.section_name = MyTable.Line_name " \
                        "     and d.proc_set_type not like  'FIELD%' " \
                        " )MyTable2 " \
                        " Left join fuseim_document c " \
                        " on c.last_condition like MyTable2.line_name " \
                        " order by MyTable2.geom_segment_id , MyTable2.line_alias,  MyTable2.point_type"

        queryProcess = queryProcess2.format(segment_id_set_string1)

        print queryProcess
        mssql_cursor2 = pMsSqlConn.cursor(as_dict=True)
        mssql_cursor2.execute(queryProcess)
        vProcessProduct = ''
        old_segmentId=''
        old_v_linealias=''
        old_v_point_type = ''
        lstProc_Set_type = []

        for row in mssql_cursor2:
            v_geom_segmentid = row["geom_segment_id"]
            v_linealias = row["line_alias"]
            v_line_name = row["line_name"]
            v_procSetType = str(row["proc_set_type"])
            v_point_type = str(row["point_type"])

            if 'FIELD' not in str(v_procSetType).upper() and v_procSetType != '' and v_procSetType != None and v_procSetType != "None":
                if old_segmentId == v_geom_segmentid and v_linealias == old_v_linealias and v_point_type == old_v_point_type:
                    if v_procSetType != None and v_procSetType != '' and 'FIELD' not in str(v_procSetType).upper():
                        # if v_procSetType not in vProcessProduct:
                        if v_procSetType  not in lstProc_Set_type:
                            vProcessProduct = vProcessProduct + ' / ' + v_procSetType
                            lstProc_Set_type.append(v_procSetType)

                            vProcessProduct = vProcessProduct.lstrip(' / ')
                            if "2D" in v_survyeType:
                                worksht2DProcessData.write((rownum2dProcess-2), 7,vProcessProduct)
                            elif "3D" in v_survyeType:
                                worksht3DProcessData.write((rownum3dProcess-2), 6, vProcessProduct)

                else:
                    v_stPnt = ''
                    v_endPnt= ''
                    v_survyeName=''
                    v_survyeType=''
                    vProcessProduct=''
                    v_locationType=''

                    lstProc_Set_type = []
                    if v_geom_segmentid in pDict_Geom.keys():
                        v_stPnt = pDict_Geom[v_geom_segmentid]["min_pnt_cnt"]
                        v_endPnt = pDict_Geom[v_geom_segmentid]["max_pnt_cnt"]
                    if v_geom_segmentid in dict_dataHolder.keys():
                        v_survyeName = dict_dataHolder[v_geom_segmentid]["survey_name"]
                        v_survyeType = dict_dataHolder[v_geom_segmentid]["product_type"]
                        #vProcessProduct = dict_dataHolder[v_geom_segmentid]["proc_set_type"]
                        vProcessProduct = ""
                        v_locationType = v_point_type

                    if v_procSetType != None and v_procSetType != '' and v_procSetType != "None" and 'FIELD' not in str(v_procSetType).upper() :
                        vProcessProduct = v_procSetType
                        lstProc_Set_type.append(v_procSetType)
                    #     if v_procSetType not in vProcessProduct:
                    #         vProcessProduct = vProcessProduct + ' / ' + v_procSetType
                    #vProcessProduct = vProcessProduct.lstrip(' / ')

                    if  '2D' in v_survyeType:
                        data2dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_linealias, v_stPnt, v_endPnt,vProcessProduct)
                        worksht2DProcessData.write_row("A" + str(rownum2dProcess), data2dprocess)
                        rownum2dProcess = rownum2dProcess + 1
                    elif '3D' in v_survyeType:
                        data3dprocess = (v_survyeName, v_survyeType, v_locationType, v_line_name, v_stPnt, v_endPnt,vProcessProduct)
                        worksht3DProcessData.write_row("A" + str(rownum3dProcess), data3dprocess)
                        rownum3dProcess = rownum3dProcess + 1

            old_segmentId = v_geom_segmentid
            old_v_linealias = v_linealias
            old_v_point_type = v_point_type

        del mssql_cursor2

        print "\n Finished: %s - Processing Process Data Sheeet Finished.\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

        RowNumstr = str(rownum2d) + ','+ str(rownum3d) + ',' + str(rownumSptData) + ',' + str(rownum2dProcess) + ',' + str(rownum3dProcess)
        return RowNumstr

    else:
        print("Error : Data sheet xls not found. ")
        return None

def Find3dLinesIntersectWithShape( pPgSqlConn, pWktPolyGon , pBlockNumber , pPolygonType,  pSrid = '4326'  ):
    """
        Returns the list of 3D (BIN) lines which are intersect to shape
        Args             :
        pPgSqlConn       : Postgres Connection
        pWktPolyGon      : (wkt polygon string)
        pSrid            :  SRID
        Returns:
           list : segment id list which are intersected to polygon
    """
    #pWktPolyGon ='POLYGON((56.1283402421783 26.198543480471578,56.107423637813326 26.183905398107484,56.118987186235245 26.200264908944106,56.123909341128174 26.231347728066144,56.1283402421783 26.198543480471578))'
    lstSegmentID_Ret= []

    if pPolygonType == 1:
        strsql = "  SELECT mytable.featnum, segment_id " \
                 " from ( " \
                 "           select featnum , segment_id, ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "            from  ( " \
                 "                     select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "                     from ( " \
                 "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point " \
                 "                               )as r  group by segment_id " \
                 "                     ) as t " \
                 "                    CROSS JOIN " \
                 "                    ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) as r1 " \
                 "          ) as mytable " \
                 "          where mytable.the_geom not like '%EMPTY%'"

                # "                    ( select the_geom as polygon from concessions where license1 like 'BLOCK {0}' limit 1 ) as r1 " \


        strsql1 = strsql.format(pBlockNumber)
    else:
        strsql = "  SELECT mytable.featnum, segment_id " \
                 " from ( " \
                 "           select featnum , segment_id, ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "            from  ( " \
                 "                     select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "                     from ( " \
                 "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point " \
                 "                               )as r  group by segment_id " \
                 "                     ) as t " \
                 "                    CROSS JOIN " \
                 "                    ST_GeomFromText('{0}',{1}) AS polygon " \
                 "          ) as mytable " \
                 "          where mytable.the_geom not like '%EMPTY%'"

        strsql1 = strsql.format(pWktPolyGon, pSrid )

    print strsql1

    try:
        pg_cursor = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        res = execute_query(pg_cursor, strsql1)
        res = pg_cursor.fetchall()

        for row in res:
            if row["segment_id"] not in lstSegmentID_Ret:
                lstSegmentID_Ret.append(row["segment_id"])

    except:
        print ("Error > Error while fetching 3d insecting records.")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        return None

    return lstSegmentID_Ret


# Added SERVER_CREATE_CSV_PATH parameter on 15 - Nov -2018
#def Get_Dict_3D_DictGeom_and_DictGeomNew(pPgsqlConn,pDICT_GEOM_FROM_CSV_3D,pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pSegment_id_set):
def Get_Dict_3D_DictGeom_and_DictGeomNew(pPgsqlConn, pDICT_GEOM_FROM_CSV_3D, pSERVER_TEST_FLG,
                                             pSERVER_CREATE_CSV_FLG, pSegment_id_set , pSERVER_CREATE_CSV_PATH , pBlockNumber,pPolygonType):
    """
            Returns the list of 3D (BIN) lines which are intersect to shape
            Args                        :
            pPgSqlConn                  : Postgres Connection
            pDICT_GEOM_FROM_CSV_3D      : Flag . False when run on server , True when you want to read dat a from CSV
            pSERVER_TEST_FLG            :  Server test flag , true while running on server
            pSERVER_CREATE_CSV_FLG      :  Create CSV flag , True if you wnat to create csv file
            pSegment_id_set             :   Segement id Set
            Returns:
               Dictionary of 3D Segment min and max point numbers
        """
    dict_geom_new_ret = {}
    dict_geom_ret = {}
    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    # pSERVER_TEST_FLG = True        # For Local Test
    # pDICT_GEOM_FROM_CSV_3D = False

    try:
        if not pDICT_GEOM_FROM_CSV_3D:

            if pSERVER_CREATE_CSV_FLG == True:
                #fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentIDList_Min_Max_3D.csv', 'w+')

                #Added on 15- Nov- 2018
                if pPolygonType == 1:
                    fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_3D_BLK' + str(pBlockNumber) + '.csv', 'w+')
                else:
                    fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_3D_' + 'WKT' + '.csv', 'w+')


                fsv.write('segment_id,start_pnt,End_pnt,Segment_Total_pnt_cnt, pnt_diff')



            set_size = 5000
            total_queryCouunt = len (pSegment_id_set) / set_size

            for seg in pSegment_id_set:
                seg_nb += 1
                seg_query += 1
                segment_id_set_string1 += "\'%s\'," % seg

                if pSERVER_TEST_FLG == True:
                    if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                        segment_id_set_string1 = segment_id_set_string1[:-1]
                        query_count += 1

                        # Points in Polygon
                        # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                        query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                                "where segment_id in (%s) group by segment_id order by segment_id" % segment_id_set_string1

                        # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                        print query
                        print ("\nProcessing query count " + str(query_count ) + " /  " + str( total_queryCouunt ) )

                        pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                        res = execute_query(pgsql_cursor, query)
                        res = pgsql_cursor.fetchall()

                        print "\n%s - Finished: Request spatial database for 3D segment information....." % strftime(
                            "%Y-%m-%d %H:%M:%S",
                            localtime())


                        for row in res:
                            row_count += 1
                            # dict_geom_ret[row_count] = row
                            dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': ['min_pnt_cnt'],
                                                        'max_pnt_cnt': ['max_pnt_cnt'], 'pnt_cnt': ['pnt_cnt'],
                                                        'pnt_diff': ['pnt_diff']}
                            if row['segment_id'] not in dict_geom_new_ret.keys():
                                dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                                        'min_pnt_cnt': row['min_pnt_cnt'],
                                                                        'max_pnt_cnt': row['max_pnt_cnt'],
                                                                        'pnt_cnt': row['pnt_cnt'],
                                                                        'pnt_diff': row['pnt_diff'], 'pnt_list': list()}

                            if pSERVER_CREATE_CSV_FLG == True:
                                fsv.write('\n' + str(row['segment_id']) +  ',' + str(
                                    row['min_pnt_cnt']) + ',' + str(row['max_pnt_cnt']) + ',' + str(row['pnt_cnt'])  + ',' + str(row['pnt_diff']))

                        print "%s - %i Points fit in the given polygon\n" % (
                        strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

                        pgsql_cursor.close()

                        print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

                        # pgsql_conn.close()
                        if row_count == 0:
                            continue

                        # # Load Point lst
                        # queryPnts = "select segment_id ,point from seis_segment_geom where segment_id in (%s) order by segment_id , point ;" % segment_id_set_string1
                        # print queryPnts
                        #
                        # print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                        #                                                                                 localtime())
                        # pgsql_cursor2 = pPgsqlConn.cursor()
                        # res = execute_query(pgsql_cursor2, queryPnts)
                        # res = pgsql_cursor2.fetchall()
                        #
                        #
                        #
                        # for row in res:
                        #     # dict_geom_ret[row_count] = row
                        #     if row[0] in dict_geom_new_ret.keys():
                        #         dict_geom_new_ret[row[0]]['pnt_list'].append(int(row[1]))
                        #
                        #     if pSERVER_CREATE_CSV_FLG == True:
                        #         fsv4.write('\n' + row[0] + ',' + str(row[1]))
                        #
                        # pgsql_cursor2.close()

            if pSERVER_CREATE_CSV_FLG == True:
                fsv.close()

            # if pSERVER_CREATE_CSV_FLG == True:
            #     fsv4.close()
        else:
            with open(G_DICT_GEOM, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    if row['segment_id'] not in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                                'min_pnt_cnt': row['min_pnt_cnt'],
                                                                'max_pnt_cnt': row['max_pnt_cnt'],
                                                                'pnt_cnt': row['pnt_cnt'],
                                                                'pnt_diff': row['pnt_diff'],
                                                                'pnt_list': list()}

                        dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': ['min_pnt_cnt'],
                                                    'max_pnt_cnt': ['max_pnt_cnt'], 'pnt_cnt': ['pnt_cnt'],
                                                    'pnt_diff': ['pnt_diff']}

                del reader

            with open(G_DICT_GEOM_POINTS, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    if row['segment_id'] in dict_geom_new_ret.keys():
                        dict_geom_new_ret[row['segment_id']]['pnt_list'].append(int(row['pnt_no']))

                del reader
    except:
        print("Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_geom_ret, dict_geom_new_ret

def Get_Dict_3D_DictGeom_and_DictGeomNewList(pPgsqlConn,pDICT_GEOM_FROM_CSV_3D,pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pSegment_id_set):
    """
            Returns the list of 3D (BIN) lines which are intersect to shape
            Args                        :
            pPgSqlConn                  : Postgres Connection
            pDICT_GEOM_FROM_CSV_3D      : Flag . False when run on server , True when you want to read dat a from CSV
            pSERVER_TEST_FLG            :  Server test flag , true while running on server
            pSERVER_CREATE_CSV_FLG      :  Create CSV flag , True if you wnat to create csv file
            pSegment_id_set             :   Segement id Set
            Returns:
               Dictionary of 3D Segment min and max point numbers
        """
    dict_geom_ret = []
    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    # pSERVER_TEST_FLG = True        # For Local Test
    # pDICT_GEOM_FROM_CSV_3D = False

    try:
        if not pDICT_GEOM_FROM_CSV_3D:

            if pSERVER_CREATE_CSV_FLG == True:
                fsv = open(SERVER_CREATE_CSV_PATH + 'dict_geom_test_FromDB_3D.csv', 'w+')
                fsv.write('segment_id,pnt_cnt,min_pnt_cnt,max_pnt_cnt,pnt_diff')

            # if pSERVER_CREATE_CSV_FLG == True:
            #     fsv4 = open(SERVER_CREATE_CSV_PATH + 'dict_geom_Pnts_FromDB.csv', 'w+')
            #     fsv4.write('segment_id,pnt_no')
            set_size = 5000
            total_queryCouunt = len (pSegment_id_set) / set_size

            for seg in pSegment_id_set:
                seg_nb += 1
                seg_query += 1
                segment_id_set_string1 += "\'%s\'," % seg

                if pSERVER_TEST_FLG == True:
                    if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                        segment_id_set_string1 = segment_id_set_string1[:-1]
                        query_count += 1

                        if (query_count > 4):          # For TEsting Purpose
                            break
                        # Points in Polygon
                        # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                        query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                                "where segment_id in (%s) group by segment_id order by segment_id" % segment_id_set_string1

                        # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                        print query
                        print ("\nProcessing query count " + str(query_count ) + " /  " + str( total_queryCouunt ) )

                        pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                        res = execute_query(pgsql_cursor, query)
                        res = pgsql_cursor.fetchall()

                        dict_geom_ret = dict_geom_ret + res

                        print "\n%s - Finished: Request spatial database for 3D segment information....." % strftime(
                            "%Y-%m-%d %H:%M:%S",
                            localtime())

                        pgsql_cursor.close()

                        print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

    except:
        print("Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_geom_ret

def Find2dLinesIntersectWithShape( pPgSqlConn, pWktPolyGon , pSERVER_TEST_FLG, pSrid = '4326' , pSegment_id_set = None ):
    """
        Returns the list of 3D (BIN) lines which are intersect to shape
        Args             :
        pPgSqlConn       : Postgres Connection
        pWktPolyGon      : (wkt polygon string)
        pSrid            :  SRID
        Returns:
           list : segment id list which are intersected to polygon
    """
    #pWktPolyGon ='POLYGON((56.1283402421783 26.198543480471578,56.107423637813326 26.183905398107484,56.118987186235245 26.200264908944106,56.123909341128174 26.231347728066144,56.1283402421783 26.198543480471578))'
    lstSegmentID_Ret= []
    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    for seg in pSegment_id_set:
        seg_nb += 1
        seg_query += 1
        segment_id_set_string1 += "\'%s\'," % seg

        if pSERVER_TEST_FLG == True:
            if seg_nb % 10000 == 0 or seg_nb == len(pSegment_id_set):
                segment_id_set_string1 = segment_id_set_string1[:-1]
                query_count += 1

                strsql = "  SELECT distinct   segment_id, (ST_Dump((mytable.the_geom))).path[1] as lineNo, " \
                         " ST_AsText(ST_StartPoint((ST_Dump(mytable.the_geom)).geom) ) as stPoint ," \
                         " ST_AsText(ST_EndPoint((ST_Dump(mytable.the_geom)).geom) ) as EndPoint " \
                         " from ( " \
                         "           select featnum , segment_id, ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                         "            from  ( " \
                         "                     select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                         "                     from ( " \
                         "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id  like '%-BIN-%'  order by point " \
                         "                               )as r  group by segment_id " \
                         "                     ) as t " \
                         "                    CROSS JOIN " \
                         "                    ST_GeomFromText('{0}',{1}) AS polygon " \
                         "          ) as mytable " \
                         "          where mytable.the_geom not like '%EMPTY%'"

                #strsql1 = strsql.format(pWktPolyGon, pSrid , segment_id_set_string1)
                # "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id in({2})  order by point " \
                strsql1 = strsql.format(pWktPolyGon, pSrid)
                print strsql1

                try:
                    pg_cursor = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                    res = execute_query(pg_cursor, strsql1)
                    res = pg_cursor.fetchall()

                    for row in res:
                        if row["segment_id"] not in lstSegmentID_Ret:     # Will Not work Here  make combination of
                            lstSegmentID_Ret.append(row["segment_id"])

                except:
                    print ("Error > Error while fetching 3d insecting records.")
                    print "------------------------------------------------------------------"
                    print "Unexpected error:", sys.exc_info()[0]
                    print traceback.print_exc()
                    return None

    return lstSegmentID_Ret


def Get_Dict_DictGeom_and_DictGeomNewWoLoop( pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV  , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG ):

    dict_geom_new_ret = {}
    dict_geom_ret ={}
    row_count = 0;
    listSegment = []
    segmentSet = set()

    pWkt = "POLYGON((55.76 18.61,55.88 18.68,55.88 18.64,55.923 18.637,55.90 18.61,55.80 18.52 ,55.76 18.61 ))"
    pWkt = "POLYGON((55.877548336531504 18.598974285367447, 55.754242053628445 18.637505671978186, 55.867736504294726 18.76337489437895, 55.97711108191402 18.653155520713305, 55.877548336531504 18.598974285367447))"

    try:
        # if not pDICT_GEOM_FROM_CSV:
        #     if pSERVER_TEST_FLG == True:
                # Points in Polygon
                # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
                query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                        "where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id order by segment_id" % pWkt

                # query = "select min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326)) group by segment_id;" % pWkt
                print query

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
                res = execute_query(pgsql_cursor, query)
                dict_geom_ret = pgsql_cursor.fetchall()

                dict_geom_new_ret = dict_geom_ret

                print "\n%s - Finished: Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",localtime())

                pgsql_cursor.close()
                # pgsql_conn.close()
                # if row_count == 0:
                #     exit(1)

                # Load Point lst
                queryPnts = "select segment_id from seis_segment_geom where ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) group by segment_id" % pWkt

                print queryPnts

                print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                localtime())
                pgsql_cursor2 = pPgsqlConn.cursor()
                res2 = execute_query(pgsql_cursor2, queryPnts)
                dict_geom_new_ret1 = pgsql_cursor2.fetchall()

                string_list = map(' '.join, dict_geom_new_ret1)
                segmentSet = set(string_list)


                pgsql_cursor2.close()

    except:
        print( "Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_geom_ret , dict_geom_new_ret , segmentSet

# Code added on 03 -Jan - 2018

def N_GetGisPointRangesInsideBlock(pPolygonType , pgsql_conn, pointFreq, segId , pConcession_nb , pWkt ):

    queryTxt4GidPntRng ="";
    srid = 4326

    if pPolygonType == 1 :
        queryTxt4GidPntRng = "select m.segment_id , m.stPoint , m.endPoint ,  n.stPoint -  m.endPoint  as difff , (m.endPoint - m.stPoint)+1 as pntcnt " \
                             " from (select startTab.segment_id, startTab.stPoint , endtab.endPoint , endtab.rno " \
                             " from " \
                             " 	(select  endtab1.segment_id , endtab1.point as endPoint , row_number() over ( order by endtab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "          from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point + {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as endtab1 " \
                             " 	) as endtab " \
                             " 	inner join " \
                             " 	(select  starttab1.segment_id , starttab1.point as stPoint , row_number() over ( order by starttab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point - {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as starttab1	" \
                             " 	) as startTab " \
                             " 	on " \
                             " 	startTab.segment_id = startTab.segment_id" \
                             " 	and startTab.rno=endTab.rno " \
                             " ) as m " \
                             " left outer join " \
                             " (select startTab.segment_id, startTab.stPoint , endtab.endPoint , endtab.rno " \
                             " from " \
                             " 	(select  endtab1.segment_id , endtab1.point as endPoint , row_number() over ( order by endtab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point + {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		 ) as endtab1 " \
                             " 	) as endtab " \
                             " 	inner join " \
                             " 	(select  starttab1.segment_id , starttab1.point as stPoint , row_number() over ( order by starttab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, (select geom from concessions where blocknumber ='{2}' limit 1 ) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point - {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as starttab1	" \
                             " 	) as startTab " \
                             " 	on " \
                             " 	startTab.segment_id = startTab.segment_id" \
                             " 	and startTab.rno=endTab.rno " \
                             " ) as n " \
                             " on m.rno = n.rno -1 "

        queryTxt4GidPntRng = queryTxt4GidPntRng.format(segId, pointFreq , pConcession_nb )

    else:
        queryTxt4GidPntRng = "select m.segment_id , m.stPoint , m.endPoint ,  n.stPoint -  m.endPoint  as difff , (m.endPoint - m.stPoint)+1 as pntcnt " \
                             " from (select startTab.segment_id, startTab.stPoint , endtab.endPoint , endtab.rno " \
                             " from " \
                             " 	(select  endtab1.segment_id , endtab1.point as endPoint , row_number() over ( order by endtab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "          from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point + {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as endtab1 " \
                             " 	) as endtab " \
                             " 	inner join " \
                             " 	(select  starttab1.segment_id , starttab1.point as stPoint , row_number() over ( order by starttab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point - {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as starttab1	" \
                             " 	) as startTab " \
                             " 	on " \
                             " 	startTab.segment_id = startTab.segment_id" \
                             " 	and startTab.rno=endTab.rno " \
                             " ) as m " \
                             " left outer join " \
                             " (select startTab.segment_id, startTab.stPoint , endtab.endPoint , endtab.rno " \
                             " from " \
                             " 	(select  endtab1.segment_id , endtab1.point as endPoint , row_number() over ( order by endtab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point + {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		 ) as endtab1 " \
                             " 	) as endtab " \
                             " 	inner join " \
                             " 	(select  starttab1.segment_id , starttab1.point as stPoint , row_number() over ( order by starttab1.point) as rno " \
                             " 	from (select l.point, l.segment_id " \
                             "         from " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as l" \
                             "             left outer join " \
                             "             (select point , segment_id  " \
                             "             from v_seis_geom_info  where ST_Within(the_geom, ST_GeomFromText( '{2}',{3}) )" \
                             "             and segment_id in( '{0}')" \
                             "             ) as r " \
                             "             on l.point - {1}  = r.point" \
                             "             and l.segment_id = r.segment_id" \
                             "             where r.point is null" \
                             " 		) as starttab1	" \
                             " 	) as startTab " \
                             " 	on " \
                             " 	startTab.segment_id = startTab.segment_id" \
                             " 	and startTab.rno=endTab.rno " \
                             " ) as n " \
                             " on m.rno = n.rno -1 "

        queryTxt4GidPntRng = queryTxt4GidPntRng.format(segId, pointFreq, pWkt , srid )

    print queryTxt4GidPntRng
    print "\n%s - Request spatial database for Intersection.." % strftime("%Y-%m-%d %H:%M:%S", localtime())
    listPntRangeInside = []
    pgsql_cursor4 = pgsql_conn.cursor()
    pgsql_cursor4.execute(queryTxt4GidPntRng)
    res = pgsql_cursor4.fetchall()
    for row in res:
        print row
        # strRange = "(" + str(row["stpoint"]) + "," + str(endpoint) + ")"
        d = TempLateClasses.clsRngDetais(str(row[0]), str(row[1]), str(row[2]), str(row[3]), str(row[4]))
        listPntRangeInside.append(d)
    pgsql_cursor4.close()
    return listPntRangeInside

#Get Seis file ranges function
def N_Get_PppdmSeisFileRange(pSegid, mssql_conn , pIsBIN):
    dict_ppdm_seisFileRng = {}

    if  pIsBIN :
        strQuery4SqlSeisFileRng = "SELECT geom_segment_id, seis_file_id, original_file_name,  ffid_start ,ffid_end , sp_start, sp_end " \
                                  ",(ffid_end - ffid_start)+1 as ffidcount, (sp_end- sp_start)+1 as sppointcnt , remark " \
                                  " ,location_reference,store_id, header_format ,  point_type , proc_set_type, processing_name , section_name , product_type " \
                                  " FROM v_SeisExtractDataNew  where geom_segment_id='{0}'"
    else :
        strQuery4SqlSeisFileRng = "SELECT startTab.geom_segment_id, startTab.seis_file_id, startTab.original_file_name,  startTab.ffid_start ,entTab.ffid_end , startTab.sp_start, entTab.sp_end, startTab.rno" \
                                  ",(entTab.ffid_end - startTab.ffid_start)+1 as ffidcount, (entTab.sp_end- startTab.sp_start)+1 as sppointcnt , startTab.remark" \
                                  ",startTab.location_reference,startTab.store_id,startTab. header_format ,startTab. point_type , startTab.proc_set_type, startTab.processing_name , startTab.section_name , startTab.product_type " \
                                  "FROM " \
                                  "	(select  ffid_end , sp_end, geom_segment_id , seis_file_id , original_file_name, remark, location_reference,store_id, header_format ,  point_type, proc_set_type, processing_name, section_name, product_type, row_number() over(order by geom_segment_id, ffid_end) as rno" \
                                  "	from (select distinct l.ffid_end , l.sp_end, l.geom_segment_id , l.seis_file_id , l.original_file_name , l.remark ,  l.location_reference, l.store_id, l.header_format ,  l.point_type , l.proc_set_type, l.processing_name, l.section_name , l.product_type " \
                                  "		from v_SeisExtractDataNew as l" \
                                  "		    left outer join v_SeisExtractDataNew as r on  l.ffid_start + 1 = r.ffid_start " \
                                  "		    and l.geom_segment_id = r.geom_segment_id" \
                                  "		    and l.seis_file_id = r.seis_file_id" \
                                  "		where " \
                                  "		r.ffid_start is null " \
                                  "		and  " \
                                  "		l.geom_segment_id in( '{0}')" \
                                  "		) as entTab1" \
                                  "	)as entTab" \
                                  "	inner join " \
                                  "	(select ffid_start , sp_start,  geom_segment_id, seis_file_id, original_file_name, remark ,location_reference,store_id, header_format ,  point_type, proc_set_type, processing_name, section_name, product_type, row_number() over(order by geom_segment_id, ffid_start) as rno" \
                                  "	from ( select distinct l.ffid_start , l.sp_start, l.geom_segment_id , l.seis_file_id , l. original_file_name , l.remark , l.location_reference, l.store_id, l.header_format ,  l.point_type , l.proc_set_type, l.processing_name , l.section_name , l.product_type " \
                                  "		from v_SeisExtractDataNew as l" \
                                  "		    left outer join v_SeisExtractDataNew as r on l.ffid_start - 1= r.ffid_start" \
                                  "		    and l.geom_segment_id = r.geom_segment_id" \
                                  "		    and l.seis_file_id = r.seis_file_id" \
                                  "		where " \
                                  "		r.ffid_start is null " \
                                  "		and " \
                                  "		l.geom_segment_id in( '{0}')" \
                                  "		)as startTab1" \
                                  "	) as startTab " \
                                  "	on " \
                                  "	startTab.geom_segment_id = startTab.geom_segment_id" \
                                  "	and startTab.rno=entTab.rno " \
                                  " order by startTab.geom_segment_id, startTab.ffid_start "

    strQuery4SqlSeisFileRng = strQuery4SqlSeisFileRng.format(pSegid)

    print strQuery4SqlSeisFileRng

    mssql_cursor = mssql_conn.cursor(as_dict=True)
    mssql_cursor.execute(strQuery4SqlSeisFileRng)
    row_count = 0;

    for row in mssql_cursor:
        row_count += 1;
        dict_ppdm_seisFileRng[row_count] = row

    print ("\n Finished" + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Get_Dict_Dict_ppdm Request PPDM #")
    del mssql_cursor
    return dict_ppdm_seisFileRng


#Function created on 02 April 2019
#Get Seis file ranges function return dictionary of all segement
def N_Get_PppdmSeisFileRange_Dict(pSegidsStr, mssql_conn ):

    dict_ppdm_seisFileRng_All_Ret = {}

    print ("\n Request " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - N_Get_PppdmSeisFileRange_Dict Request PPDM #")

    strQuery4SqlSeisFileRng = "select * from  T_v_SeisExtractDataNew2 where geom_segment_id in({0}) order by geom_segment_id , ffid_start "

    strQuery4SqlSeisFileRng = strQuery4SqlSeisFileRng.format(pSegidsStr)

    print strQuery4SqlSeisFileRng

    mssql_cursor = mssql_conn.cursor(as_dict=True)
    mssql_cursor.execute(strQuery4SqlSeisFileRng)
    row_count = 0;

    segmenid = "";
    oldsegmenid = "";

    dict_ppdm_seisFileRng = {}

    for row in mssql_cursor:
        row_count += 1;
        segmenid = row["geom_segment_id"];

        if oldsegmenid== "" :
            dict_ppdm_seisFileRng = {}
            dict_ppdm_seisFileRng[row_count] = row
            oldsegmenid = segmenid
            #dict_ppdm_seisFileRng_All_Ret[segmenid] = dict_ppdm_seisFileRng

        elif segmenid == oldsegmenid :
            dict_ppdm_seisFileRng[row_count] = row
            oldsegmenid = segmenid

        elif segmenid != oldsegmenid:
            dict_ppdm_seisFileRng_All_Ret[oldsegmenid] = dict_ppdm_seisFileRng
            dict_ppdm_seisFileRng = {}
            dict_ppdm_seisFileRng[row_count] = row
            oldsegmenid = segmenid

    dict_ppdm_seisFileRng_All_Ret[segmenid] = dict_ppdm_seisFileRng

    print ("\n Finished" + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - N_Get_PppdmSeisFileRange_Dict Request PPDM #")
    del mssql_cursor

    return dict_ppdm_seisFileRng_All_Ret


def N_Get_PppdmSeisFileRange4BIN(pSegid, mssql_conn):
    dict_ppdm_seisFileRng = {}
    strQuery4SqlSeisFileRng = "SELECT geom_segment_id, seis_file_id, original_file_name,  ffid_start ,ffid_end , sp_start, sp_end " \
                            ",(ffid_end - ffid_start)+1 as ffidcount, (sp_end- sp_start)+1 as sppointcnt , remark " \
                            " ,location_reference,store_id, header_format ,  point_type , proc_set_type, processing_name , section_name " \
                            " FROM v_SeisExtractDataNew  where geom_segment_id='{0}'"

    strQuery4SqlSeisFileRng = strQuery4SqlSeisFileRng.format(pSegid)

    print strQuery4SqlSeisFileRng

    mssql_cursor = mssql_conn.cursor(as_dict=True)
    mssql_cursor.execute(strQuery4SqlSeisFileRng)
    row_count = 0;

    for row in mssql_cursor:
        row_count += 1;
        dict_ppdm_seisFileRng[row_count] = row

    print ("\n Finished" + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - N_Get_PppdmSeisFileRange4BIN Request PPDM #")
    del mssql_cursor
    return dict_ppdm_seisFileRng


#Function to get Point Frequency from Database postgis
def N_GetPointFreqFromPostgis(pPgsql_conn, pSeg_id ):

    poitnFreq = 0.0
    queryTxt = "select freq from  seis_segment_pointfreq where segment_id ='{0}'; ".format(pSeg_id)

    try:
        print queryTxt
        print "\n%s - Request spatial database for point Frequency." % strftime("%Y-%m-%d %H:%M:%S",localtime())
        pgsql_cursor3 = pPgsql_conn.cursor()
        pgsql_cursor3.execute(queryTxt)
        res = pgsql_cursor3.fetchall()
        for row in res:
            print row[0]
            poitnFreq = int(row[0])

        pgsql_cursor3.close()

    except:
        # print("Error : No Record Geometry Record found in database fro seg_id: %s", seg_id)
        poitnFreq = -1

    return poitnFreq

# Function aded on 02 April 2019
#Function to get Point Frequency from Database postgis  Return Dictionary
def N_GetPointFreqFromPostgis_Dict(pPgsql_conn, pSeg_idsStr ):

    dict_pnt_freq_ret = {}

    poitnFreq = 0.0
    queryTxt = "select segment_id, freq from  seis_segment_pointfreq where segment_id in ({0}); ".format(pSeg_idsStr)

    try:
        print queryTxt
        print "\n%s - Request spatial database for point Frequency." % strftime("%Y-%m-%d %H:%M:%S",localtime())
        pgsql_cursor3 = pPgsql_conn.cursor()
        pgsql_cursor3.execute(queryTxt)
        res = pgsql_cursor3.fetchall()
        for row in res:
            #print row[0]
            if row[0] not in dict_pnt_freq_ret.keys():
                dict_pnt_freq_ret[row[0]] = {'freq': row[1]}
                poitnFreq = int(row[1])

        pgsql_cursor3.close()

    except:
        # print("Error : No Record Geometry Record found in database fro seg_id: %s", seg_id)
        poitnFreq = -1

    return dict_pnt_freq_ret

# New function after modifying old function added only for BIN segements
# Added path , block number, server path variable pSERVER_CREATE_CSV_PATH on 18 Nov 2018
def N_Get_Dict_DictGisStEndOfBIN(pPgsqlConn, pSegment_id_set, pDICT_MINMAX_FROM_CSV, pSERVER_TEST_FLG,
                              pSERVER_CREATE_CSV_FLG , pSERVER_CREATE_CSV_PATH , pPolygonType , pBlockNumber ):

    dict_gis_st_end_ret = {}

    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    set_size = 10000
    #Added on 9th April 2019 ...........Raj
    totalSegCnt = len(pSegment_id_set)
    
    total_queryCouunt = totalSegCnt / set_size
    
    if (totalSegCnt % set_size ) > 0:
        total_queryCouunt = total_queryCouunt + 1 ;
    
    #Comment this while local run with csv file
    #pSERVER_TEST_FLG = True  # For Server query fetch  test on with local run  oterwise comment it.

    if not pDICT_MINMAX_FROM_CSV:
        for seg in pSegment_id_set:
            if '-BIN' in seg:
                seg_nb += 1
                seg_query += 1
                segment_id_set_string1 += "\'%s\'," % seg

                if pSERVER_TEST_FLG == True:
                    if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                        segment_id_set_string1 = segment_id_set_string1[:-1]
                        query_count += 1
                        print query_count

                        query = "select segment_id, min(point) as g_seg_stPnt, max(point) as g_seg_endPnt  from seis_segment_geom where segment_id in(%s) group by segment_id;" % \
                                segment_id_set_string1

                        # print ("\n %s - Request spatial database for min max point....Query Count %d / %d ." , strftime("%Y-%m-%d %H:%M:%S",
                        #                                                                            localtime()) , query_count , total_queryCouunt )
                        print ("\n " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Request spatial database for min max point....Query Count " + str(query_count) + " / " + str(total_queryCouunt) )
                        print query
                        pgsql_cursor2 = pPgsqlConn.cursor()
                        pgsql_cursor2.execute(query)
                        res = pgsql_cursor2.fetchall()

                        if pSERVER_CREATE_CSV_FLG == True:
                            # fsv4 = open('/home/raj/OGDR/Test_csv/New/dict_MinMax_FromDB.csv', 'w+')
                            # fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt')

                            if pPolygonType == 1:
                                fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_BLK' + str(pBlockNumber) + '.csv', 'w+')
                            else:
                                fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_' + 'WKT' + '.csv', 'w+')

                            fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt')

                        for row in res:
                            row_count += 1
                            if row[0] not in dict_gis_st_end_ret.keys():
                                dict_gis_st_end_ret[row[0]] = {'segment_id': row[0], 'g_seg_stPnt': row[1],
                                                           'g_seg_endPnt': row[2]}

                                if pSERVER_CREATE_CSV_FLG == True:
                                    fsv4.write('\n' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]))


                        del pgsql_cursor2

        if pSERVER_CREATE_CSV_FLG == True:
            fsv4.close()

            # print("\n Finished: %s - Result return for query count %d / %d \n" , (strftime("%Y-%m-%d %H:%M:%S", localtime())) , query_count , total_queryCouunt)
            print ("\n Finished: " + (strftime("%Y-%m-%d %H:%M:%S",
                                     localtime())) + " - Request spatial database for min max point....Query Count " + str(
                query_count) + " / " + str(total_queryCouunt))


    else:
        ##################################################################################
        # When Ignoring Database Query Part
        ##################################################################################
        with open(G_DICT_MIN_MAX_NAME , 'r') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                if row['segment_id'] not in dict_gis_st_end_ret.keys():
                    dict_gis_st_end_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                          'g_seg_stPnt': row['g_seg_stPnt'],
                                                          'g_seg_endPnt': row['g_seg_endPnt']}

            del reader

    return dict_gis_st_end_ret


# Added processType_ProcessFieldAll parameter on 01 - Oct -2018
# Added SERVER_CREATE_CSV_PATH parameter on 15 - Nov -2018
#def Get_Dict_DictGeom_and_DictGeomNew( pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV  , pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG , pBlockNumber , pPolygonType):
#def Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV, pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType , pProcessTypeFlg):
#def Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV, pSERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType, pProcessTypeFlg,pProductTypeFlg ):
def N_Get_Dict_DictGeom_and_DictGeomNew(pWkt, pPgsqlConn, pDICT_GEOM_FROM_CSV_Flg, pDICT_GEOM_FROM_CSV,
                                          pSERVER_CREATE_CSV_FLG, pBlockNumber, pPolygonType, pProcessTypeFlg,
                                          pProductTypeFlg , pSERVER_CREATE_CSV_PATH ):
    ret_segment_id_set = set()
    query = ""
    dict_geom_ret = {}

    try:
        if not pDICT_GEOM_FROM_CSV_Flg:

            # Points in Polygon
            # query = "select segment_id from seis_segment_geom where ST_Within(the_geom,ST_GeomFromText('%s',4326));" % pWkt
            if pPolygonType == 1:
                if (pProcessTypeFlg == 'ALL'):
                    if pProductTypeFlg == 'ALL':
                        #query = "select distinct segment_id from v_seis_geom_info where segment_id not like '%-BIN-%' and ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) ) ".format(pBlockNumber)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pBlockNumber)
                    else:
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) ".format(pProductTypeFlg, pBlockNumber)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and product_type like '{0}%' and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pBlockNumber)

                elif (pProcessTypeFlg == 'PROCESS'):
                    if pProductTypeFlg == 'ALL' :
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '%s' limit 1 ) )" % pBlockNumber
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{0}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pBlockNumber)
                    else:
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) ".format(pProductTypeFlg, pBlockNumber)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                " where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pBlockNumber)

                elif (pProcessTypeFlg == 'FIELD'):
                    if pProductTypeFlg == 'ALL':
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '%s' limit 1 ) ) " % pBlockNumber
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{0}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pBlockNumber)
                    else:
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) ".format(pProductTypeFlg, pBlockNumber)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type not in ('CMP', 'BIN') and ST_Within(the_geom, (select geom from concessions where blocknumber = '{1}' limit 1 ) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pBlockNumber)

                #----------------------------------------------------------------------------------------------------
                # Testing Query ..Put segement id for specific segemnt
                # query = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from seis_segment_geom " \
                #         "where ST_Within(the_geom, (select geom from concessions where blocknumber like '%s' limit 1 ) ) and segment_id in ('OXY96SAB1-Other-SW106-1') group by segment_id order by segment_id" % pBlockNumber
                # # TEsting Query End
                # ----------------------------------------------------------------------------------------------------

            else:
                if (pProcessTypeFlg == 'ALL'):
                    if pProductTypeFlg == 'ALL':
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #          "where segment_id not like '%-BIN-%' and ST_Within(the_geom, ST_GeomFromText( '{0}',4326) ) ".format(pWkt)
                        query1 = "select segment_id, count(*) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and ST_Within(the_geom, ST_GeomFromText( '{0}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pWkt)
                    else:
                        # query = "select distinct segment_id  from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) ".format(pProductTypeFlg, pWkt)
                        query1 = "select segment_id, count(*) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and product_type like '{0}%' and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pWkt)

                elif (pProcessTypeFlg == 'PROCESS'):
                    if pProductTypeFlg == 'ALL':
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) " % pWkt
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{0}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pWkt)
                    else:
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) " .format(pProductTypeFlg, pWkt)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pWkt)
                elif (pProcessTypeFlg == 'FIELD'):
                    if pProductTypeFlg == 'ALL':
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '%s',4326) ) " % pWkt
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where segment_id not like '%-BIN-%' and location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{0}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pWkt)
                    else:
                        # query = "select distinct segment_id from v_seis_geom_info " \
                        #         "where segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) ".format(pProductTypeFlg, pWkt)
                        query1 = "select segment_id, count( *) pnt_cnt, min(point)as min_pnt_cnt, max(point) as max_pnt_cnt, (max(point) - min(point)) + 1 as pnt_diff from v_seis_geom_info " \
                                "where  segment_id not like '%-BIN-%' and product_type like '{0}%' and location_type not in ('CMP', 'BIN') and  ST_Within(the_geom, ST_GeomFromText( '{1}',4326) ) group by segment_id order by segment_id"
                        query = query1.format(pProductTypeFlg, pWkt)

            print query
            print "\n%s - Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                            localtime())
            pgsql_cursor = pPgsqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
            res = execute_query(pgsql_cursor, query)
            res = pgsql_cursor.fetchall()

            print "\n%s - Finished: Request spatial database for segment information....." % strftime("%Y-%m-%d %H:%M:%S",
                                                                                                      localtime())
            print "%s - Result return\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()))

            fsv = None;

            if pSERVER_CREATE_CSV_FLG == True:
                # fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List.csv', 'w+')
                if pPolygonType == 1:
                    fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_OthrBIN_BLK' + str(pBlockNumber) + '.csv', 'w+')
                else:
                    fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_OthrBIN_' + 'WKT' + '.csv', 'w+')

                fsv.write('segment_id, min_pnt_cnt,max_pnt_cnt, pnt_cnt , pnt_diff')

            row_count = 0 ;
            for row in res:
                row_count += 1
                # dict_geom_ret[row_count] = row
                ret_segment_id_set.add(row['segment_id'])

                dict_geom_ret[row_count] = {'segment_id': row['segment_id'], 'min_pnt_cnt': row['min_pnt_cnt'],
                                            'max_pnt_cnt': row['max_pnt_cnt'], 'pnt_cnt': row['pnt_cnt'],'pnt_diff': row['pnt_diff']}

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv.write('\n' + str(row['segment_id']) + ',' + str(
                        row['min_pnt_cnt']) + ',' + str(row['max_pnt_cnt']) + ',' + str(row['pnt_cnt']) + ',' + str(row['pnt_diff']))
                    # fsv.write('\n' + str(row['segment_id']) )

                #print row_count

            print "%s - %i Points fit in the given polygon\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

            if pSERVER_CREATE_CSV_FLG == True:
                fsv.close()

            pgsql_cursor.close()

        else:
            #print( "\nReading Segment List :")
            with open( pDICT_GEOM_FROM_CSV , 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    ret_segment_id_set.add(row['segment_id'])
                    print(str(row['segment_id']))

                #del reader

    except:
        print( "Error in Reading PostGis data or Geometry Dictionary records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)


    return ret_segment_id_set , dict_geom_ret

def N_Find3dLinesIntersectWithShape( pPgSqlConn, pWktPolyGon , pBlockNumber , pPolygonType,  pSERVER_CREATE_CSV_FLG,  pSERVER_CREATE_CSV_PATH, pSrid = '4326'  ):
    """
        Returns the list of 3D (BIN) lines which are intersect to shape
        Args             :
        pPgSqlConn       : Postgres Connection
        pWktPolyGon      : (wkt polygon string)
        pSrid            :  SRID
        Returns:
           list : segment id list which are intersected to polygon
    """
    #pWktPolyGon ='POLYGON((56.1283402421783 26.198543480471578,56.107423637813326 26.183905398107484,56.118987186235245 26.200264908944106,56.123909341128174 26.231347728066144,56.1283402421783 26.198543480471578))'
    lstSegmentID_Ret= []
    segment_id_set_bin_overlap_ret = set()

    if pPolygonType == 1:
        strsql = "  SELECT mytable.featnum, segment_id " \
                 " from ( " \
                 "           select featnum , segment_id, ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "            from  ( " \
                 "                     select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "                     from ( " \
                 "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point " \
                 "                               )as r  group by segment_id " \
                 "                     ) as t " \
                 "                    CROSS JOIN " \
                 "                    ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) as r1 " \
                 "          ) as mytable " \
                 "          where mytable.the_geom not like '%EMPTY%'"

                # "                    ( select the_geom as polygon from concessions where license1 like 'BLOCK {0}' limit 1 ) as r1 " \


        strsql1 = strsql.format(pBlockNumber)
    else:
        strsql = "  SELECT mytable.featnum, segment_id " \
                 " from ( " \
                 "           select featnum , segment_id, ST_AsText(ST_Intersection(linestring, polygon)) As the_geom " \
                 "            from  ( " \
                 "                     select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring " \
                 "                     from ( " \
                 "                               select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point " \
                 "                               )as r  group by segment_id " \
                 "                     ) as t " \
                 "                    CROSS JOIN " \
                 "                    ST_GeomFromText('{0}',{1}) AS polygon " \
                 "          ) as mytable " \
                 "          where mytable.the_geom not like '%EMPTY%'"

        strsql1 = strsql.format(pWktPolyGon, pSrid )

    print strsql1

    try:

        fsv = None;
        if pSERVER_CREATE_CSV_FLG == True:
            # fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List.csv', 'w+')
            if pPolygonType == 1:
                fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_BIN_Ovrlp_BLK' + str(pBlockNumber) + '_3D.csv','w+')
            else:
                fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List_BIN_Ovrlp_' + 'WKT_3D' + '.csv', 'w+')

        pg_cursor = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        res = execute_query(pg_cursor, strsql1)
        res = pg_cursor.fetchall()

        for row in res:
            if row["segment_id"] not in lstSegmentID_Ret:
                lstSegmentID_Ret.append(row["segment_id"])

                segment_id_set_bin_overlap_ret.add(row["segment_id"] );

                if pSERVER_CREATE_CSV_FLG == True:
                    fsv.write('\n' + str(row['segment_id']) )

        if pSERVER_CREATE_CSV_FLG == True:
            fsv.close()

    except:
        print ("Error > Error while fetching 3d insecting records.")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        return None

    return lstSegmentID_Ret , segment_id_set_bin_overlap_ret


def N_Find3dLinesWithInShape( pPgSqlConn, pWktPolyGon , pBlockNumber , pPolygonType,  pSERVER_CREATE_CSV_FLG,  pSERVER_CREATE_CSV_PATH, pProcessTypeFlg, pProductTypeFlg,  pSrid = '4326'  ):
    """
        Returns the list of 3D (BIN) lines which are with in to shape
        Args             :
        pPgSqlConn       : Postgres Connection
        pWktPolyGon      : (wkt polygon string)
        pSrid            :  SRID
        Returns:
           list : segment id list which are intersected to polygon
    """
    #pWktPolyGon ='POLYGON((56.1283402421783 26.198543480471578,56.107423637813326 26.183905398107484,56.118987186235245 26.200264908944106,56.123909341128174 26.231347728066144,56.1283402421783 26.198543480471578))'
    lstSegmentID_Ret= []
    dict_withInShape_ret = {}

    if pPolygonType == 1:
        strsql = "  select t.featnum , t.segment_id ,  t.min_pnt ,t.max_pnt" \
                 " from ( " \
                 "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring ,  min(point) min_pnt ,max(point) max_pnt  " \
                 "          from  ( " \
                 "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%' and ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) ) order by point  " \
                 "                )as r  group by segment_id " \
                 "       ) as t  " \
                 " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "

        strsql1 = strsql.format(pBlockNumber)
    else:
        strsql = "  select t.featnum , t.segment_id , t.min_pnt ,t.max_pnt " \
                 " from ( " \
                 "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
                 "          from  ( " \
                 "                  select point, segment_id, orig_lon_x, orig_lat_y, the_geom from v_seis_geom_info where segment_id LIKE '%-BIN-%' and ST_Within(the_geom, (ST_GeomFromText('{0}',{1})) ) order by point  " \
                 "                )as r  group by segment_id " \
                 "       ) as t  " \
                 " where ST_Within(t.linestring, ( ST_GeomFromText('{0}',{1})) ) "

        strsql1 = strsql.format(pWktPolyGon, pSrid )

        print strsql1



    # if pPolygonType == 1:
    #     strsql = "  select t.featnum , t.segment_id " \
    #              " from ( " \
    #              "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring  " \
    #              "          from  ( " \
    #              "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point  " \
    #              "                )as r  group by segment_id " \
    #              "       ) as t  " \
    #              " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #
    #     strsql1 = strsql.format(pBlockNumber)
    # else:
    #     strsql = "  select t.featnum , t.segment_id " \
    #              " from ( " \
    #              "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring  " \
    #              "          from  ( " \
    #              "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where segment_id LIKE '%-BIN-%'  order by point  " \
    #              "                )as r  group by segment_id " \
    #              "       ) as t  " \
    #              " where ST_Within(t.linestring, ( ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #
    #     strsql1 = strsql.format(pWktPolyGon, pSrid )

    # vlocType = ''
    #
    # if pProductTypeFlg == '2D':
    #     vlocType = "\'CMP\'"
    # elif pProductTypeFlg == 'ALL' or pProductTypeFlg == '3D':
    #     vlocType = "\'CMP',\'BIN'"

    # if pPolygonType == 1:
    #     if (pProcessTypeFlg == 'ALL'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #             strsql1 = strsql.format(pBlockNumber)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where product_type='{1}' and  ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #             strsql1 = strsql.format(pBlockNumber, pProductTypeFlg)
    #     elif (pProcessTypeFlg == 'PROCESS'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type in ({1}) and ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #             strsql1 = strsql.format(pBlockNumber, vlocType)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type in ({2}) and product_type='{1}' and  ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #             strsql1 = strsql.format(pBlockNumber, pProductTypeFlg, vlocType)
    #     elif (pProcessTypeFlg == 'FIELD'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type not in ({1}) and ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #             strsql1 = strsql.format(pBlockNumber, vlocType)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type not in ({2}) and product_type='{1}' and  ST_Within(the_geom, (select geom from concessions where blocknumber ='{0}' limit 1 ) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, ( select geom as polygon from concessions where blocknumber ='{0}' limit 1 ) ) "
    #
    #             strsql1 = strsql.format(pBlockNumber , pProductTypeFlg , vlocType )
    # else:
    #     strsql = "  select t.featnum , t.segment_id , ,t.min_pnt, t.max_pnt " \
    #              " from ( " \
    #              "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #              "          from  ( " \
    #              "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from seis_segment_geom where ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #              "                )as r  group by segment_id " \
    #              "       ) as t  " \
    #              " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #
    #     strsql1 = strsql.format(pWktPolyGon, pSrid )
    #
    #     if (pProcessTypeFlg == 'ALL'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #             strsql1 = strsql.format(pWktPolyGon, pSrid)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where product_type='{2}' and  ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #             strsql1 = strsql.format(pWktPolyGon, pSrid , pProductTypeFlg )
    #     elif (pProcessTypeFlg == 'PROCESS'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type in ({2}) and ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #             strsql1 = strsql.format(pWktPolyGon, pSrid , vlocType)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type in ({3}) and product_type='{2}' and  ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #             strsql1 = strsql.format(pWktPolyGon, pSrid , pProductTypeFlg, vlocType)
    #     elif (pProcessTypeFlg == 'FIELD'):
    #         if pProductTypeFlg == 'ALL':
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type not in ({2}) and ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #             strsql1 = strsql.format(pWktPolyGon, pSrid , vlocType)
    #         else:
    #             strsql = "  select t.featnum , t.segment_id ,t.min_pnt, t.max_pnt " \
    #                      " from ( " \
    #                      "          select row_number() over (ORDER BY R.segment_id) as featnum, r.segment_id, ST_MakeLine(r.the_geom ORDER BY r.point) as linestring , min(point) min_pnt ,max(point) max_pnt " \
    #                      "          from  ( " \
    #                      "                  select point, segment_id, orig_lon_x, orig_lat_y , the_geom from v_seis_geom_info where location_type not in ({3}) and product_type='{2}' and  ST_Within(the_geom, (ST_GeomFromText('{0}',{1}) AS polygon) )  order by point  " \
    #                      "                )as r  group by segment_id " \
    #                      "       ) as t  " \
    #                      " where ST_Within(t.linestring, (ST_GeomFromText('{0}',{1}) AS polygon) ) "
    #
    #             strsql1 = strsql.format(pWktPolyGon, pSrid  , pProductTypeFlg , vlocType )
    #
    # print strsql1

    try:

        fsv = None;
        if pSERVER_CREATE_CSV_FLG == True:
            # fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_Start_End_Pnt_List.csv', 'w+')
            if pPolygonType == 1:
                fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_BIN_WithIn_BLK_Lst' + str(pBlockNumber) + '.csv','w+')
            else:
                fsv = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_BIN_WithIn_BLK_Lst' + 'WKT' + '.csv', 'w+')

            fsv.write('segment_id,min_pnt,max_pnt')

        pg_cursor = pPgSqlConn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        res = execute_query(pg_cursor, strsql1)
        res = pg_cursor.fetchall()

        row_count = 0 ;

        for row in res:
            minpnt_t = row[2];
            maxpnt_t = row[3];
            if (maxpnt_t - minpnt_t) <> 0 :
                if row["segment_id"] not in lstSegmentID_Ret:
                    lstSegmentID_Ret.append(row["segment_id"])

                row_count += 1
                if row[1] not in dict_withInShape_ret.keys():
                    dict_withInShape_ret[row[0]] = {'segment_id': row[1], 'min_pnt': row[2],
                                                   'max_pnt': row[3]}
                    if pSERVER_CREATE_CSV_FLG == True:
                        fsv.write('\n' + str(row['segment_id']) + ',' +  str(row['min_pnt']) + ',' +  str(row['max_pnt']) )

        if pSERVER_CREATE_CSV_FLG == True:
            fsv.close()

    except:
        print ("Error > Error while fetching 3d insecting records.")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        return None

    return lstSegmentID_Ret , dict_withInShape_ret


def N_Get_Dict_DictGisStEnd(pPgsqlConn, pSegment_id_set, pDICT_MINMAX_FROM_CSV, pSERVER_TEST_FLG,
                              pSERVER_CREATE_CSV_FLG , pSERVER_CREATE_CSV_PATH , pPolygonType , pBlockNumber ):

    dict_gis_st_end_ret = {}

    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0

    set_size = 10000
    total_queryCouunt = len(pSegment_id_set) / set_size

    #Comment this while local run with csv file
    #pSERVER_TEST_FLG = True  # For Server query fetch  test on with local run  oterwise comment it.

    if not pDICT_MINMAX_FROM_CSV:
        for seg in pSegment_id_set:
            seg_nb += 1
            seg_query += 1
            segment_id_set_string1 += "\'%s\'," % seg

            if pSERVER_TEST_FLG == True:
                if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                    segment_id_set_string1 = segment_id_set_string1[:-1]
                    query_count += 1
                    print query_count

                    # query = "select segment_id, min(point) as g_seg_stPnt, max(point) as g_seg_endPnt  from seis_segment_geom where segment_id in(%s) group by segment_id;" % \
                    #         segment_id_set_string1
                    query = "select segment_id, segminpnt as g_seg_stPnt, segmaxpnt as g_seg_endPnt , Total_pnt from v_segment_minmx where segment_id in(%s)" % \
                            segment_id_set_string1

                    # print ("\n %s - Request spatial database for min max point....Query Count %d / %d ." , strftime("%Y-%m-%d %H:%M:%S",
                    #                                                                            localtime()) , query_count , total_queryCouunt )

                    segment_id_set_string1 = "";

                    print ("\n " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Request spatial database for min max point....Query Count " + str(query_count) + " / " + str(total_queryCouunt) )
                    print query
                    pgsql_cursor2 = pPgsqlConn.cursor()
                    pgsql_cursor2.execute(query)
                    res = pgsql_cursor2.fetchall()

                    if pSERVER_CREATE_CSV_FLG == True:
                        # fsv4 = open('/home/raj/OGDR/Test_csv/New/dict_MinMax_FromDB.csv', 'w+')
                        # fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt')

                        if pPolygonType == 1:
                            fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_BLK' + str(pBlockNumber) + '.csv', 'w+')
                        else:
                            fsv4 = open(pSERVER_CREATE_CSV_PATH + 'SegmentID_GIS_Start_End_Pnt_List_' + 'WKT' + '.csv', 'w+')

                        fsv4.write('segment_id,g_seg_stPnt,g_seg_endPnt,Total_pnt')

                    for row in res:
                        row_count += 1
                        if row[0] not in dict_gis_st_end_ret.keys():
                            dict_gis_st_end_ret[row[0]] = {'segment_id': row[0], 'g_seg_stPnt': row[1],'g_seg_endPnt': row[2] , 'Total_pnt': row[3]  }

                            if pSERVER_CREATE_CSV_FLG == True:
                                fsv4.write('\n' + str(row[0]) + ',' + str(row[1]) + ',' + str(row[2]) +  ',' + str(row[3]))

                    del pgsql_cursor2

        if pSERVER_CREATE_CSV_FLG == True:
            fsv4.close()

            # print("\n Finished: %s - Result return for query count %d / %d \n" , (strftime("%Y-%m-%d %H:%M:%S", localtime())) , query_count , total_queryCouunt)
            print ("\n Finished: " + (strftime("%Y-%m-%d %H:%M:%S",
                                     localtime())) + " - Request spatial database for min max point....Query Count " + str(
                query_count) + " / " + str(total_queryCouunt))


    else:
        ##################################################################################
        # When Ignoring Database Query Part
        ##################################################################################
        with open(G_DICT_MIN_MAX_NAME , 'r') as f:
            reader = csv.DictReader(f)
            row_count = 0
            for row in reader:
                if row['segment_id'] not in dict_gis_st_end_ret.keys():
                    dict_gis_st_end_ret[row['segment_id']] = {'segment_id': row['segment_id'],
                                                          'g_seg_stPnt': row['g_seg_stPnt'],
                                                          'g_seg_endPnt': row['g_seg_endPnt']}

            del reader

    return dict_gis_st_end_ret



def Get_Dict_Dict_ppdm_From_T_SeisEx( pMssql_conn, pSegment_id_set, pDICT_PPDM_FROM_CSV,pSERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG , pSERVER_CREATE_CSV_PATH ,
                                               pPolygonType ,
                                               pBlockNumber ):

    dict_ppdm_ret = {}
    segment_id_set_string1 = ""
    seg_nb = 0
    seg_query = 0
    query_count = 0
    row_count = 0


    #pSERVER_TEST_FLG = True  # For Server query fetch  test on with local run  oterwise comment it.

    try:
        set_size = 10000
        #Added on 9 th April 2019 
        totalSetCnt = len(pSegment_id_set)
        if totalSetCnt >= set_size:
            total_queryCouunt = totalSetCnt / set_size
            if (totalSetCnt % set_size) >  0:
                total_queryCouunt = total_queryCouunt + 1;
        else:
            total_queryCouunt =1;

        if total_queryCouunt == 0 :
            total_queryCouunt=1;

        if not pDICT_PPDM_FROM_CSV:
            for seg in pSegment_id_set:
                seg_nb += 1
                seg_query += 1
                segment_id_set_string1 += "\'%s\'," % seg

                if pSERVER_TEST_FLG == True:
                    if seg_nb % set_size == 0 or seg_nb == len(pSegment_id_set):
                        segment_id_set_string1 = segment_id_set_string1[:-1]
                        query_count += 1

                        # print segment_id_set_string1
                        query = "select geom_segment_id ,seis_file_id, section_name," \
                                " processing_name , proc_set_type,store_id,location_reference,original_file_name,header_format,point_type,product_type ,remark," \
                                " min(ffid_start) min_ffid_start,max(ffid_end) max_ffid_end," \
                                " min(sp_start) min_sp_start,max(sp_end) max_sp_end " \
                                " from  T_v_SeisExtractDataNew2" \
                                " where geom_segment_id in (%s) " \
                                " group by geom_segment_id, seis_file_id , section_name ," \
                                " processing_name , proc_set_type,store_id,location_reference,original_file_name,header_format, " \
                                " point_type,product_type , remark " \
                                " order by geom_segment_id , min_ffid_start " % segment_id_set_string1

                        print query

                        print ("\n " + (strftime("%Y-%m-%d %H:%M:%S", localtime())) +" - Request Get_Dict_Dict_ppdm_From_T_SeisEx #" + str( query_count ) + " / " + str( total_queryCouunt ) + "  - "+ str(seg_query ) + " segments queried" )

                        segment_id_set_string1 = ""
                        seg_query = 0

                        mssql_cursor = pMssql_conn.cursor(as_dict=True)
                        mssql_cursor.execute(query)

                        if pSERVER_CREATE_CSV_FLG == True:
                            if pPolygonType == 1 :
                                fsv2 = open(pSERVER_CREATE_CSV_PATH + 'dict_ppdm_FromDB_BLK'+ str(pBlockNumber)+ '.csv', 'w+')
                            else:
                                fsv2 = open(pSERVER_CREATE_CSV_PATH + 'dict_ppdm_FromDB_WKT.csv', 'w+')

                            fsv2.write('geom_segment_id,seis_file_id,section_name,processing_name,proc_set_type,store_id,location_reference,original_file_name,header_format,point_type,product_type,remark,min_ffid_start,max_ffid_end,min_sp_start,max_sp_end')

                        for row in mssql_cursor:
                            row_count += 1
                            dict_ppdm_ret[row_count] = row

                            if pSERVER_CREATE_CSV_FLG == True:
                                strWr = ''
                                strWr = str(row['geom_segment_id']) + ',' + str(row['seis_file_id']) + ',' + str(
                                    row['section_name']) + ',' + str(row['processing_name']) + ','
                                strWr = strWr + str(row['proc_set_type']) + ',' + str(
                                    row['store_id']) + ',' + str(row['location_reference']) + ','
                                strWr = strWr + str(row['original_file_name']) + ',' + str(
                                    row['header_format']) + ',' + str(row['point_type']) + ',' + str(
                                    row['product_type']) + ',' + str(row['remark']) + ','
                                strWr = strWr + str(row['min_ffid_start']) + ','
                                strWr = strWr + str(row['max_ffid_end']) + ',' + str(row['min_sp_start']) + ',' + str(
                                    row['max_sp_end'])

                                fsv2.write('\n' + strWr)

                        print ("\n Finished" + (strftime("%Y-%m-%d %H:%M:%S", localtime())) + " - Get_Dict_Dict_ppdm_From_T_SeisEx Request PPDM #" + str(
                            query_count) + " / " + str(total_queryCouunt) + "  - " + str(
                            seg_query) + " segments queried")

                        del mssql_cursor

            if pSERVER_CREATE_CSV_FLG == True:
                fsv2.close()
        else:
            with open(G_DICT_PPDM, 'r') as f:
                reader = csv.DictReader(f)
                row_count = 0
                for row in reader:
                    row_count += 1
                    dict_ppdm_ret[row_count] = row

        print "\n %s - %i Records found in FUSEIM_SEIS_MATCH\n" % (strftime("%Y-%m-%d %H:%M:%S", localtime()), row_count)

    except:
        print( "Error in Reading PPDM records or CSV  file")
        print "------------------------------------------------------------------"
        print "Unexpected error:", sys.exc_info()[0]
        print traceback.print_exc()
        exit(1)

    return dict_ppdm_ret


# test = {}
# if 'uidcdn--2' in [x for v in d.values() for y in v.values()] :
#     print "found:--------------"
#     print x
#     print v
#     test = v
# else:
#     print "2Nfond"
#
# if len(test) > 0 :
#     print test["segment_id"]
#     print test["min_pnt_cnt"]
#     print test["max_pnt_cnt"]
#     print test["pnt_diff"]
# else:
#     print "Test Not found"






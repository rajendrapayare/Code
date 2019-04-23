import constants_sgy

from time import gmtime, strftime , localtime
import pymssql
import LogWriter
import Utilities
import DBUtilities
import segy
import ProcessTif8Files
import os

import Cls_HeaderReader
import Cls_Pgssql

PRODUCT_TYPE_2D = ['2D Detail', '2D Swath', '2D']
PRODUCT_TYPE_3D = ['3D Detail', '3D Swath', '3D']
TCONN = '172.16.10.34'
TPORT = None
TUSER = 'ppdmx'
TPASS = 'dataman'
TDB = 'ppdm_ogdr_mirror'
XSDIR = '/var/lib/xstreamline'
MEDIADIR = '/media/'


class cls_MainProcess:

    def processMain( self, pgsql_conn , pDictParm) :

        # Create CSV files on Server
        SERVER_CREATE_CSV_FLG = False  # For using this flag SERVER_TEST_FLG must be True
        SERVER_CREATE_CSV_PATH = '/home/'  # Path to create csv file of database list points which we can to avoid query on databases

        # Flag to set csv from database or CSV
        # Make this flag false wihile running on server
        # DICT_GEOM_FROM_CSV = True
        # DICT_PPDM_FROM_CSV = True
        # DICT_MINMAX_FROM_CSV = True
        # DICT_GEOM_FROM_CSV_3D = True

        DICT_GEOM_FROM_CSV = False
        DICT_PPDM_FROM_CSV = False
        DICT_MINMAX_FROM_CSV = False
        DICT_GEOM_FROM_CSV_3D = False


        concession_nb = pDictParm["concession_nb"]
        fldrNameBlkNoWKt = pDictParm["fldrNameBlkNoWKt"]
        input_path = pDictParm["input_path"]
        output_path = pDictParm["output_path"]
        pPolygon_type = pDictParm["pPolygon_type"]
        processType_ProcessFieldAll = pDictParm["processType_ProcessFieldAll"]
        productType_2Dor3D = pDictParm["productType_2Dor3D"]
        wkt = pDictParm["wkt"]

        SERVER_TEST_FLG = pDictParm["SERVER_TEST_FLG"]
        G_SEIS_FORMAT_FILE = pDictParm["G_SEIS_FORMAT_FILE"]

        DEFAULTPNTRANGETHERSHOLD = pDictParm["DEFAULTPNTRANGETHERSHOLD"]

        ################################################################
        # Create CSV File for Report
        foutCsvFileName = output_path + 'Segment_Intersections_Report_' + fldrNameBlkNoWKt + '.csv'
        clslog = LogWriter.clsLogWriter(foutCsvFileName)
        clslog.writeLine("Meera Seismic Spatial based Extract Utility")
        clslog.writeLine("\nProcess Start Time :, %s \n" % strftime("%Y-%m-%d %H:%M:%S", localtime()))
        clslog.WriteHeaderine()


        ################################################################
        # Create CSV File for Log file Report
        foutLogFileName = output_path + 'Segment_ExtractionsReport_' + fldrNameBlkNoWKt + '.rpt'
        if (foutLogFileName == None):
            print ("Unable to crete xls report file %s", foutLogFileName)
            exit(1)

        clslog_log = LogWriter.clsLogWriter(foutLogFileName)
        clslog_log.writeLine("Meera Seismic Spatial based Extract Utility")
        clslog_log.writeLine("\nProcess Start Time :, %s \n" % strftime("%Y-%m-%d %H:%M:%S", localtime()))

        ################################################################


        ## Settings required Parameters
        print("Processing Required Parameters ...................")
        ## Read CSV file segments Header Format
        y = Cls_HeaderReader.cls_SegYCSVFormat()

        if SERVER_TEST_FLG == True:
            dict_segyFormat = y.fromCsvFile(G_SEIS_FORMAT_FILE)
        else:
            dict_segyFormat = y.fromCsvFile('/home/raj/OGDR/seis_format.csv')

        ## Sies format input file missing ...
        if dict_segyFormat == False:
            print "seis_format.csv reading error or File not found."
            exit();

        ################################################################################################

        dict_geom = {}
        dict_geom_new = {}
        row_count = 0
        # Get data from post gis database in dictionary format
        # dict_geom, dict_geom_new = DBUtilities.Get_Dict_DictGeom_and_DictGeomNew( pWkt=wkt , pPgsqlConn = pgsql_conn, pDICT_GEOM_FROM_CSV=DICT_GEOM_FROM_CSV, pSERVER_TEST_FLG=SERVER_TEST_FLG , pSERVER_CREATE_CSV_FLG= SERVER_CREATE_CSV_FLG)
        # if productType_2Dor3D == 'ALL' or productType_2Dor3D == '2D':
        if productType_2Dor3D == 'ALL' or productType_2Dor3D == '2D' or productType_2Dor3D == '3D':
            # dict_geom, dict_geom_new = DBUtilities.Get_Dict_DictGeom_and_DictGeomNew(pWkt=wkt, pPgsqlConn=pgsql_conn,pDICT_GEOM_FROM_CSV=DICT_GEOM_FROM_CSV,pSERVER_TEST_FLG=SERVER_TEST_FLG,pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,pBlockNumber=concession_nb,pPolygonType=pPolygon_type)
            # Added processType_ProcessFieldAll parameter on 01 - Oct -2018
            dict_geom, dict_geom_new = DBUtilities.Get_Dict_DictGeom_and_DictGeomNew(pWkt=wkt, pPgsqlConn=pgsql_conn,
                                                                                     pDICT_GEOM_FROM_CSV=DICT_GEOM_FROM_CSV,
                                                                                     pSERVER_TEST_FLG=SERVER_TEST_FLG,
                                                                                     pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                                                                                     pBlockNumber=concession_nb,
                                                                                     pPolygonType=pPolygon_type,
                                                                                     pProcessTypeFlg=processType_ProcessFieldAll,
                                                                                     pProductTypeFlg=productType_2Dor3D)
        segment_id_set = set()
        for v in dict_geom.itervalues():
            segment_id_set.add(v['segment_id'])
        ### Get 3D Segement from Database spatial Query
        # =========================================================
        if SERVER_TEST_FLG == True:
            if productType_2Dor3D == 'ALL' or productType_2Dor3D == '3D':
                if processType_ProcessFieldAll == 'ALL' or processType_ProcessFieldAll == 'PROCESS':
                    # lst_3DIntersectSegement = DBUtilities.Find3dLinesIntersectWithShape( pPgSqlConn=pgsql_conn , pWktPolyGon=wkt, pSrid='4326')
                    lst_3DIntersectSegement = DBUtilities.Find3dLinesIntersectWithShape(pPgSqlConn=pgsql_conn,
                                                                                        pWktPolyGon=wkt,
                                                                                        pBlockNumber=concession_nb,
                                                                                        pPolygonType=pPolygon_type,
                                                                                        pSrid='4326')

                    # lst_3DIntersectSegement1 = DBUtilities.Find2dLinesIntersectWithShape(pPgSqlConn=pgsql_conn, pWktPolyGon=wkt,
                    #                                                                    pSERVER_TEST_FLG=SERVER_TEST_FLG, pSrid='4326' , pSegment_id_set= segment_id_set)

                    segment_id_set3D = set()

                    if len(lst_3DIntersectSegement) > 0:
                        for mm in range(len(lst_3DIntersectSegement)):
                            segment_id_set.add(lst_3DIntersectSegement[mm])

                            if lst_3DIntersectSegement[mm] not in dict_geom_new.keys():
                                segment_id_set3D.add(lst_3DIntersectSegement[mm])

                    ### Get dictionary fromm post gis for 3D Segement from Database spatial Query
                    dict_geom_3D = {}
                    dict_geom_new_3d = {}

                    if len(segment_id_set3D) > 0:
                        dict_geom_3D, dict_geom_new_3D = DBUtilities.Get_Dict_3D_DictGeom_and_DictGeomNew(
                            pPgsqlConn=pgsql_conn,
                            pDICT_GEOM_FROM_CSV_3D=DICT_GEOM_FROM_CSV_3D,
                            pSERVER_TEST_FLG=SERVER_TEST_FLG,
                            pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG,
                            pSegment_id_set=segment_id_set3D)

                    ### Add 3D dictionary items into main directories.
                    dictCount = len(dict_geom_3D)
                    if dictCount > 0:
                        for v_3d in dict_geom_3D.itervalues():
                            dictCount += 1
                            dict_geom[dictCount] = v_3d

                        dictCount1 = len(dict_geom_new_3D)
                        if dictCount1 > 0:
                            for v_3d in dict_geom_new_3D.itervalues():
                                if v_3d["segment_id"] not in dict_geom_new.keys():
                                    dict_geom_new[v_3d["segment_id"]] = v_3d
        # =========================================================
        # PPDM part Create Sql Connection.....
        mssql_conn = pymssql.connect(server=TCONN, user=TUSER, password=TPASS, database=TDB, login_timeout=20)
        mssql_cursor = mssql_conn.cursor(as_dict=True)
        # seg_nb = 0
        # seg_query = 0
        # query_count = 0
        # row_count = 0
        dict_listPntRangeInside = {}
        dict_ppdm = {}
        segment_id_set_string = ""
        ###############################################################
        # DICT_PPDM_FROM_CSV = True;                # For csv flag to avoind sql query
        ################################################################
        # Get Cdictionary of PPDM data using this function
        dict_ppdm = DBUtilities.Get_Dict_Dict_ppdm(pMssql_conn=mssql_conn, pSegment_id_set=segment_id_set,
                                                   pDICT_PPDM_FROM_CSV=DICT_PPDM_FROM_CSV,
                                                   pSERVER_TEST_FLG=SERVER_TEST_FLG,
                                                   pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG)
        if len(dict_ppdm) == 0:
            exit(1)
        ###############################################################################
        # Get Min Max Point from GIS database for 3D point Utilisation
        # for seg in segment_id_set:
        #     segment_id_set_string += "\'%s\'," % seg
        dict_gis_st_end = {}
        dict_gis_st_end = DBUtilities.Get_Dict_DictGisStEnd(pPgsqlConn=pgsql_conn, pSegment_id_set=segment_id_set,
                                                            pDICT_MINMAX_FROM_CSV=DICT_MINMAX_FROM_CSV,
                                                            pSERVER_TEST_FLG=SERVER_TEST_FLG,
                                                            pSERVER_CREATE_CSV_FLG=SERVER_CREATE_CSV_FLG)
        segment_id_set_string = ""
        pgsql_conn.close()
        # Get/ select distinct  Dataset_id  from above result......
        dict_dataset_temp = {}
        dict_dataset = {}
        row_count = 0
        # for dd in dict_ppdm.itervalues():
        #     if not dd['dataset_id'] in dict_dataset_temp:
        #         dict_dataset_temp[dd['dataset_id']] = {dd['dataset_id']}
        #         row_count += 1
        #         dict_dataset[row_count] ={'dataset_id': dd['dataset_id'],'product_type': dd['product_type'], 'survey_name': dd['survey_name'], 'processing_name': dd['processing_name'], 'proc_set_type': dd['proc_set_type'] }
        for dd in dict_ppdm.itervalues():
            if not dd['dataset_id'] in dict_dataset_temp:
                if productType_2Dor3D == '2D' and dd['product_type'] in PRODUCT_TYPE_2D:
                    dict_dataset_temp[dd['dataset_id']] = {dd['dataset_id']}
                    row_count += 1
                    dict_dataset[row_count] = {'dataset_id': dd['dataset_id'], 'product_type': dd['product_type'],
                                               'survey_name': dd['survey_name'],
                                               'processing_name': dd['processing_name'],
                                               'proc_set_type': dd['proc_set_type']}

                elif productType_2Dor3D == '3D' and dd['product_type'] in PRODUCT_TYPE_3D:
                    dict_dataset_temp[dd['dataset_id']] = {dd['dataset_id']}
                    row_count += 1
                    dict_dataset[row_count] = {'dataset_id': dd['dataset_id'], 'product_type': dd['product_type'],
                                               'survey_name': dd['survey_name'],
                                               'processing_name': dd['processing_name'],
                                               'proc_set_type': dd['proc_set_type']}
                elif productType_2Dor3D == 'ALL':
                    dict_dataset_temp[dd['dataset_id']] = {dd['dataset_id']}
                    row_count += 1
                    dict_dataset[row_count] = {'dataset_id': dd['dataset_id'], 'product_type': dd['product_type'],
                                               'survey_name': dd['survey_name'],
                                               'processing_name': dd['processing_name'],
                                               'proc_set_type': dd['proc_set_type']}
        print "List of datasets contained in the polygon:\n"
        print "Item No   Dim       Survey Name       Processing Name   Proc Set Type"
        # dataset_nb = 1
        # for k in dataset_set:
        #     print "%-10i%-18s%-18s%-40s" % (dataset_nb, k[1], k[2], k[3])
        #
        dataset_id_list_all = []
        product_type = ""
        for k, v in dict_dataset.iteritems():
            if v['product_type'] in PRODUCT_TYPE_2D:
                product_type = '2D'
            elif v['product_type'] in PRODUCT_TYPE_3D:
                product_type = '3D'
            else:
                product_type = 'unknown'
            dataset_id_list_all.append(k)

            print "%-10i%-10s%-18s%-18s%-40s" % (k, product_type, v['survey_name'], v['processing_name'],
                                                 v['proc_set_type'])
        ### Select Product type from User Input
        # productType_2Dor3D = ''
        # productType_2Dor3D = raw_input("\nSelect product Type you want to export (eg. \"2D,3D \" or \"all\" or \"none\"):\n")
        #
        # while productType_2Dor3D.upper() not in ('2D', '3D', 'NONE', 'ALL'):
        #     print"You have enter wrong Product Type ...Please Renter again."
        #     productType_2Dor3D = raw_input(
        #         "Select product Type you want to export (eg. \"2D,3D \" or \"all\" or \"none\"):\n")
        #     if productType_2Dor3D == "none" or productType_2Dor3D == "" or productType_2Dor3D.upper() == 'NONE':
        #         exit(1)
        #     elif productType_2Dor3D.upper() == "2D" or productType_2Dor3D.upper() == "3D" or productType_2Dor3D.upper() == "ALL":
        #         productType_2Dor3D = productType_2Dor3D.upper()
        #
        #
        # productType_2Dor3D = productType_2Dor3D.upper()
        ### Select Product type from User Input
        dataset_id_list_string = raw_input(
            "\nSelect Datasets you want to export (eg. \"1,3,5\" or \"all\" or \"none\"):\n")
        dataset_id_list = []
        if dataset_id_list_string == "none":
            exit(1)
        elif dataset_id_list_string == "":
            exit(1)
        elif "," in dataset_id_list_string:
            dataset_id_list_input = dataset_id_list_string.split(",")
            for i in dataset_id_list_input:
                try:
                    i = int(i)
                except ValueError:
                    print "%s not good dataset number entry\n" % i
                    exit(1)
                if i not in dict_dataset.iterkeys():
                    print "%i not in dataset list" % i
                    exit(1)
                else:
                    dataset_id_list.append(int(i))
        elif dataset_id_list_string.upper() == "ALL":
            dataset_id_list = dataset_id_list_all
        elif int(dataset_id_list_string) <= len(dict_dataset):
            dataset_id_list.append(int(dataset_id_list_string))
            print (int(dataset_id_list_string))
        else:
            print "Not good dataset number entry\n"
        list_dataset = []
        for k in dataset_id_list:
            list_dataset.append(dict_dataset[k]['dataset_id'])

        # Create Postgres Connection
        clsPgSqlConnObj = Cls_Pgssql.Cls_PgSql()
        pgsql_conn = clsPgSqlConnObj.pgsql_connect()

        #pgsql_conn = pgsql_connect()
        vFileType = None
        # For Testing Purpose Instead of Running at End
        # DBUtilities.WriteDataSheet(mssql_conn, dict_geom_new, dict_ppdm, output_path, segment_id_set, fldrNameBlkNoWKt)
        print("Processing Extractions...................")
        # Extraction:
        for k, v in dict_ppdm.iteritems():
            seg_id = v["geom_segment_id"]
            datasetID = v["dataset_id"]
            vFileType = ''
            clslog_log.writeLine("Processing Segment Id : " + seg_id)

            d = LogWriter.clsSegMentDetails(segId=seg_id)
            if (productType_2Dor3D == 'ALL') or (
                    productType_2Dor3D == '2D' and v["product_type"] in PRODUCT_TYPE_2D) or (
                    productType_2Dor3D == '3D' and v["product_type"] in PRODUCT_TYPE_3D):
                if datasetID in list_dataset:

                    if v["proc_set_type"] == 'FIELD SEGD':
                        d.remark = d.remark + 'Its a SEGD type file.;'
                        # clslog.WriteCsvLineUsingCls(clsSegDet=d)
                        vFileType = 'SEGD'

                    # If you get heaaader information from CSV file then retrive other information
                    # vSegSpStart = v["sp_start"]
                    # continue
                    # vSegSpEnd = v["sp_end"]
                    # set start_Points and end point of segement , first check stpnt(minpnt) and endpnt(maxpnt) from postgis data
                    # if not found then consider segment start point end point from database table
                    # if seg_id in dict_gis_st_end.keys():
                    #     varGisStEndPnt = dict_gis_st_end[seg_id]
                    #     vSegSpStart = varGisStEndPnt['g_seg_stPnt']  # postgis data
                    #     vSegSpEnd = varGisStEndPnt['g_seg_endPnt']  # postgis data
                    # else:

                    vSegSpStart = v["min_point_start"]  # from database table
                    vSegSpEnd = v["max_point_end"]  # from database table
                    vSeis_file_id = v['seis_file_id']

                    d.stPnt = vSegSpStart
                    d.endPnt = vSegSpEnd
                    d.seisFileID = vSeis_file_id
                    d.segFfidMin = v["min_ffid_start"]
                    d.segFfidMax = v["max_ffid_end"]
                    d.pointType = v['point_type']

                    vLocationRef = v["location_reference"]
                    vOrgnlFileName = v["original_file_name"]
                    vProcName4Fldr = v["processing_name"]
                    vProcSetType4SubFldr = v["proc_set_type"]
                    vSectName4SubFldr = v['section_name']
                    vStorId = v['store_id']
                    vLocReference = v['location_reference']
                    vPointType = v['point_type']

                    # set Points to cut from file or inside affected area
                    minPnt = 0
                    maxPnt = 0
                    if seg_id in dict_geom_new.keys():
                        varPGisData = dict_geom_new[seg_id]
                        minPnt = varPGisData["min_pnt_cnt"]
                        maxPnt = varPGisData["max_pnt_cnt"]
                    else:
                        d.Remark = 'Error: No Record found in PostGIS Data.;'

                    # Define 2d or 3d type folder
                    vType2dOr3d = ''
                    if '3D' in str(v['product_type']).upper():
                        if 'FIELD' in str(vProcSetType4SubFldr).upper():
                            vType2dOr3d = '3D_SEISMIC_FIELD_DATA'
                        else:
                            vType2dOr3d = '3D_SEISMIC_PROCESS_DATA'

                    elif '2D' in str(v['product_type']).upper():
                        if 'FIELD' in str(vProcSetType4SubFldr).upper():
                            vType2dOr3d = '2D_SEISMIC_FIELD_DATA'
                        else:
                            vType2dOr3d = '2D_SEISMIC_PROCESS_DATA'
                    else:
                        d.error = str(d.error) + 'Product Type not like 2D or 3D ;'
                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                        continue

                    print (
                        "-------------------------------------------------------------------------------------------------------------------------------------------------------")
                    print('>> vSegSpStart ', vSegSpStart, ' vSegSpEnd: ', vSegSpEnd, ' vLocationRef:', vLocationRef,
                          ' vOrgnlFileName:', vOrgnlFileName, ' vProcName4Fldr:', vProcName4Fldr,
                          ' vProcSetType4SubFldr:',
                          vProcSetType4SubFldr, ' vSectName4SubFldr:', vSectName4SubFldr)
                    print('>> minPnt:', minPnt, ' maxPnt:', maxPnt)

                    clslog_log.writeLine(
                        "\n-------------------------------------------------------------------------------------------------------------------------------------")
                    clslog_log.writeLine(
                        '\n>> vSegSpStart ' + str(vSegSpStart) + ' vSegSpEnd: ' + str(
                            vSegSpEnd) + ' vLocationRef:' + str(
                            vLocationRef) +
                        ' vOrgnlFileName:' + str(vOrgnlFileName) + ' vProcName4Fldr:' + str(vProcName4Fldr) +
                        ' vProcSetType4SubFldr :' + str(vProcSetType4SubFldr) + ' vSectName4SubFldr:' + str(
                            vSectName4SubFldr))

                    clslog_log.writeLine('\n>> minPnt:' + str(minPnt) + ' maxPnt:' + str(maxPnt))

                    # Check Source SEGY file exists or not
                    # vBlock_no='WKT'

                    if SERVER_TEST_FLG == True:
                        # input_path = '/var/lib/xstreamline/' + vStorId + '/media/' + vLocReference + '/'
                        # input_path = XSDIR + vStorId + '/'+ MEDIADIR +  vLocReference + '/'
                        input_path = XSDIR + '/' + vStorId + '/' + MEDIADIR + vLocReference + '/'
                        srcFileName = input_path + vOrgnlFileName

                        if not os.path.isfile(srcFileName):
                            # Check filepath in remarks column
                            clslog_log.writeLine(
                                '\n>>' + srcFileName + ' is not found so set from remark  as ' + v["remark"])
                            srcFileName = v["remark"]
                    else:
                        srcFileName = input_path + vOrgnlFileName

                        if not os.path.isfile(srcFileName):
                            d.error = str(d.error) + srcFileName + ' Segy File not Found ;'
                            clslog.WriteCsvLineUsingCls(clsSegDet=d)
                            continue

                    clslog_log.writeLine('\n>> srcFileName : ' + srcFileName)
                    # if (srcFileName == '/data/seismic/pl00304/prestk/data/FLD_08X4____5665.tif8'):
                    #     continue

                    # First Get Header type from ppdm dictionary information
                    headFormat = v["header_format"]
                    # headFormat='PDO-3D-3'
                    # Get Header Information from seis_format
                    if vFileType != 'SEGD':
                        try:
                            vHeaderInfo = dict_segyFormat[headFormat]
                            # print (vHeaderInfo['ILINE_FORMAT'])
                            # print (vHeaderInfo['POINT_POSITION'])

                            # vheadType = 'N' + vHeaderInfo['ILINE_FORMAT']
                            # vHeadPos = int(vHeaderInfo['POINT_POSITION']) - 1

                            ILINE_FORMAT = vHeaderInfo['ILINE_FORMAT']
                            POINT_POSITION = vHeaderInfo['POINT_POSITION']
                            POINT_MODULO = vHeaderInfo['POINT_MODULO']
                            XLINE_FORMAT = vHeaderInfo['XLINE_FORMAT']
                            POINT_FORMAT = vHeaderInfo['POINT_FORMAT']

                            # if (ILINE_FORMAT != '' and ILINE_FORMAT != None) and POINT_MODULO != '' and POINT_MODULO != None:
                            #     print("Multiple Header Information found for header % found in Header CSV file", headFormat)
                            #     d.error = str(d.error) + ';' + 'Information for header '+ str(headFormat) +' not found in Header CSV file.'
                            #     clslog.WriteCsvLineUsingCls(clsSegDet=d)
                            #     continue
                            vHeadPos = '';
                            vheadType = '';

                            if ILINE_FORMAT != '' and ILINE_FORMAT != None:
                                vheadType = 'N' + str(ILINE_FORMAT)
                                if POINT_POSITION == '' or POINT_POSITION == None:
                                    clslog_log.writeLine("Skipping segment " + seg_id + " having file name=" + str(
                                        vOrgnlFileName) + " due to wrong for header info>> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_POSITION:" + POINT_POSITION)
                                    clslog_log.writeLine(
                                        "POINT_POSITION is null or none for header format:" + headFormat)
                                    clslog_log.writeLine("Check Segment CSV header information file ")
                                    clslog_log.writeLine(
                                        "-------------------------------------------------------------------------------------")
                                    continue
                                else:
                                    vHeadPos = int(POINT_POSITION) - 1
                            elif POINT_MODULO != '' and POINT_MODULO != None:
                                vheadType = 'N' + str(POINT_MODULO)
                                if XLINE_FORMAT == '' or XLINE_FORMAT == None:
                                    clslog_log.writeLine("Skipping segment " + seg_id + " having file name=" + str(
                                        vOrgnlFileName) + " due to wrong for header info>> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_MODULO:" + POINT_MODULO)
                                    clslog_log.writeLine("XLINE_FORMAT is null or none for header format:" + headFormat)
                                    clslog_log.writeLine("Check Segment CSV header information file ")
                                    clslog_log.writeLine(
                                        "-------------------------------------------------------------------------------------")
                                    continue
                                else:
                                    vHeadPos = int(XLINE_FORMAT) - 1
                            elif (ILINE_FORMAT == '' or ILINE_FORMAT == None) and (
                                    POINT_MODULO == '' or POINT_MODULO == None) and (
                                    POINT_FORMAT != None or POINT_FORMAT != ''):
                                vheadType = 'N' + str(POINT_FORMAT)
                                vHeadPos = int(POINT_POSITION) - 1

                            if vHeadPos == '' or vHeadPos == None or vheadType == '' or vheadType == None:
                                print(
                                    "Error : Information for header %s not found in Header seis_format CSV file",
                                    headFormat)
                                d.error = str(d.error) + 'Information for header ' + str(
                                    headFormat) + ' not found in Header CSV file;'
                                clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                continue

                            clslog_log.writeLine(">>vHeadPos : " + str(vHeadPos) + " , vheadType : " + str(vheadType))

                        except KeyError:
                            print("Error : Information for header " + headFormat + " not found in Header CSV file")
                            clslog_log.writeLine(
                                "Skipping segment " + seg_id + " having file name=" + str(vOrgnlFileName))
                            clslog_log.writeLine(
                                "Information for header format:" + headFormat + " not found in Header seis_format.csv file.")
                            d.error = str(
                                d.error) + 'Information for header is not found in Header CSV seis_format.csv file;'
                            clslog_log.writeLine(
                                "-------------------------------------------------------------------------------------")
                            clslog.WriteCsvLineUsingCls(clsSegDet=d)
                            continue
                        except:
                            print(
                                    "Error : Header Information is not sufficient or wrong for header format " + headFormat + " in seis_format.csv file")
                            clslog_log.writeLine(
                                "Skipping segment " + seg_id + " having file name = " + str(vOrgnlFileName))
                            clslog_log.writeLine(
                                "Due to wrong for header info >> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_MODULO:" + POINT_MODULO)
                            clslog_log.writeLine(
                                "-------------------------------------------------------------------------------------")
                            clslog.WriteCsvLineUsingCls(clsSegDet=d)
                            continue

                    pntRangeOffest = 0
                    if '-BIN-' not in seg_id:
                        # Processing 2D Points.....................
                        print ("Processing Segment Id :", seg_id)
                        clslog_log.writeLine("Processing Segment Id : " + seg_id)
                        #####################################################
                        # Added for Range Check
                        #####################################################
                        varPGisData = dict_geom_new[seg_id]
                        minPnt = int(varPGisData["min_pnt_cnt"])
                        maxPnt = int(varPGisData["max_pnt_cnt"])
                        pntCnt = int(varPGisData["pnt_cnt"])
                        pntCntDiff = int(varPGisData["pnt_diff"])

                        # Taking Start end of segment from PPDM for 2D
                        vSegSpStart = v["min_point_start"]  # from database table
                        vSegSpEnd = v["max_point_end"]  # from database table

                        d.stPnt = vSegSpStart
                        d.endPnt = vSegSpEnd

                        listPntRangeInside = [
                            (minPnt, maxPnt)]  # Set Default range #@ Set defualt 1 range of max point in min Points

                        if pntCnt != pntCntDiff:  # Processing points having point counts difference with total points
                            if (
                                    pntCntDiff - pntCnt) > DEFAULTPNTRANGETHERSHOLD:  # @ If differnce between poinsts and range difference is greater then threshold

                                # @ Run Query to get Ranges of Points and range offfset
                                # listPnts  = GetListOfPntRangeInside4SegmentId(pSeg_id=seg_id, pWktString=wkt)
                                listPnts = dict_geom_new[seg_id]['pnt_list']
                                PointNotInSequenceRangeFlg = False
                                PointNotInSequenceRangeFlg, pntRangeOffest = DBUtilities.GetListOfPntDiffOffset(
                                    pPgsql_conn=pgsql_conn, pSeg_id=seg_id, pPnt=minPnt)

                                if (PointNotInSequenceRangeFlg == False):
                                    # pntRangeOffest =  (pntCntDiff-1)/(pntCnt-1)

                                    if pntRangeOffest <= DEFAULTPNTRANGETHERSHOLD:  # If This Ration is leass than 2 meane pointrangeOffset is 1, so we will set defualtpointRangeOffset
                                        pntRangeOffest = DEFAULTPNTRANGETHERSHOLD

                                    if (pntRangeOffest * (pntCnt - 1)) == (
                                            pntCntDiff - 1):  # If all points in affected area then no need to create ranges

                                        listPntRangeInside = [(minPnt, maxPnt)]
                                    else:
                                        listPntRangeInside = Utilities.createRanges(listPnts, int(pntRangeOffest))
                                else:
                                    if seg_id not in dict_listPntRangeInside.keys():
                                        if seg_id in dict_gis_st_end.keys():
                                            varGisStEndPnt = dict_gis_st_end[seg_id]
                                            vSegSpStart = varGisStEndPnt['g_seg_stPnt']  # postgis data
                                            vSegSpEnd = varGisStEndPnt['g_seg_endPnt']  # postgis data
                                        else:
                                            print "Run Query"
                                            vSegSpStart, vSegSpEnd = DBUtilities.GetMinMaxPointFromGIS(
                                                pPgSqlConn=pgsql_conn, pSegId=seg_id)

                                        # listPntRangeInside = DBUtilities.Get_ListOfPointRangeInsideInPolygonUsingSegID( pPgSqlConn = pgsql_conn, pSegID = seg_id,
                                        #                                                                             pWktPolyGon = wkt, pSegStPntNo = vSegSpStart,
                                        #                                                                             pSegEndPntNo = vSegSpEnd, pSrid = '4326')

                                        listPntRangeInside = DBUtilities.Get_ListOfPointRangeInsideInPolygonUsingSegID(
                                            pPgSqlConn=pgsql_conn, pSegID=seg_id,
                                            pWktPolyGon=wkt, pSegStPntNo=vSegSpStart,
                                            pSegEndPntNo=vSegSpEnd, pBlockNumber=concession_nb,
                                            pPolygonType=pPolygon_type,
                                            pSrid='4326')

                                        dict_listPntRangeInside[seg_id] = listPntRangeInside
                                    else:
                                        listPntRangeInside = dict_listPntRangeInside[seg_id]

                            else:
                                listPntRangeInside = [(minPnt, maxPnt)]

                        try:
                            dPath = Utilities.Create_Dir_Structure(fldrNameBlkNoWKt, vProcName4Fldr,
                                                                   vProcSetType4SubFldr,
                                                                   vSectName4SubFldr, output_path, vType2dOr3d)
                        except:
                            print("Error : Unable to create Folder structure.")
                            d.error = str(
                                d.error) + 'Unable to create Folder structure for .' + fldrNameBlkNoWKt + '|' + vProcName4Fldr + '|' + vProcSetType4SubFldr + '|' + vSectName4SubFldr + '|' + output_path + '|' + vType2dOr3d
                            clslog.WriteCsvLineUsingCls(clsSegDet=d)
                            continue

                        clslog_log.writeLine("\nvSegSpStart : " + str(vSegSpStart) + ", vSegSpEnd : " + str(vSegSpEnd))
                        if vFileType == 'SEGD':
                            d = ProcessTif8Files.ProcessSEGD(plistPntRangeInside=listPntRangeInside, pValues=v,
                                                             pSrcFileName=srcFileName, pMssql_conn=mssql_conn,
                                                             pDestPath=dPath, pClassLogWrtr=clslog, pClasssLogDetls=d)
                        else:
                            for pntrange in listPntRangeInside:  # @Loop For PntRanges found inside Shapes

                                d.pntRangeMin = None
                                d.pntRangeMax = None
                                d.destPath = ''
                                d.fMinPnt = None
                                d.fMaxPnt = None
                                d.remark = ''

                                str2 = str(pntrange)
                                str3 = (str2.rstrip(')')).lstrip('(')
                                lstRangPntNum = str3.split(',')

                                str3 = lstRangPntNum[0]
                                minPnt = str3.rstrip().lstrip()

                                str4 = lstRangPntNum[1]
                                maxPnt = str4.rstrip().lstrip()

                                # Assign Inside Block points range to loggger
                                d.insidePntRangeMin = minPnt
                                d.insidePntRangeMax = maxPnt

                                # Set file name as per Min Max Point
                                dstFileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy', '').replace('.segy',
                                                                                                            '') + '_' + str(
                                    minPnt) + '_' + str(maxPnt) + '_' + vPointType + '.sgy'

                                clslog_log.writeLine(
                                    "\npntrange : " + str(pntrange) + ", minPnt : " + str(minPnt) + ", maxPnt : " + str(
                                        maxPnt))

                                ffidMin = ''
                                ffidMax = ''

                                readByFFIDFlg = False  # Set Flag is flase , If file is read by Respective FFID then this flag will set as True
                                if vPointType == 'FFID':

                                    spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                        mssql_conn, seg_id, str(minPnt), str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                    # postgis(Point)  == PPDM(Point_start) ...........Usually
                                    # spStartAsFfidFlg is true means Postgis(Point)  == PPDM(Ffid_Start)
                                    # spStartAsFfidFlg >> If this flag true means post_gis (point)'s record not foound in PPDM as PPDM(Sp_Start)
                                    # insteat of that post_gis (point) present as PPDM(Ffid_Start)
                                    # So we need change or consider seg start and end as ffid start and ffid_end
                                    clslog_log.writeLine(
                                        "\nvPointType = FFID " + ", spStartAsFfidFlg :" + str(
                                            spStartAsFfidFlg) + ", ffidMin : " + str(ffidMin) + ", ffidMax : " + str(
                                            ffidMax))

                                    if spStartAsFfidFlg == 1:
                                        vSegSpStart = v["min_ffid_start"]
                                        vSegSpEnd = v["max_ffid_end"]
                                    elif spStartAsFfidFlg == 2:
                                        d.remark = str(d.remark) + 'FFID proportionaly calculated.;'
                                        clslog_log.writeLine('\nFFID proportionaly calculated.')
                                    else:
                                        d.remark = str(d.remark) + 'FFID or sp point sequence not following properly;'
                                        clslog_log.writeLine('\nFFID or sp point sequence not following properly;')
                                        continue

                                    # Added on 4 Oct - 2018 by Raj
                                    if (ffidMin == None or ffidMax == None):
                                        clslog_log.writeLine('\nFFID unbale to determined.')
                                        continue

                                    d.ffidMin = ffidMin
                                    d.ffidMax = ffidMax

                                    # Chnage file name as ffid
                                    dstFileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy', '').replace('.segy',
                                                                                                                '') + '_' + str(
                                        ffidMin) + '_' + str(ffidMax) + '_' + vPointType + '.sgy'

                                if int(minPnt) == int(vSegSpStart) and int(maxPnt) == int(vSegSpEnd):
                                    print("1 >> Segmnent Processing for Start and End point matched")
                                    print("1 >> SegStart == MinPnt -- MaxPnt ==  SegEnd")
                                    # Just Copy The File

                                    copyfile(srcFileName, dstFileName)

                                    d.pntRangeMin = minPnt
                                    d.pntRangeMax = maxPnt
                                    d.destPath = dstFileName
                                    d.fMinPnt = minPnt
                                    d.fMaxPnt = maxPnt

                                elif int(minPnt) == int(vSegSpStart) and int(maxPnt) <= int(vSegSpEnd):
                                    print(
                                        "2 >> Segmnent Processing min pnt match Start Match AND max point less than equal to end point")
                                    print("2 >> SegStart == MinPnt -- MaxPnt --  SegEnd")
                                    # Code for Polycut Testing

                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}
                                    if vPointType == 'FFID':
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)
                                    else:
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            # if vPointType == 'FFID':
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        if f.tr_written > 0:
                                            # Rename file with actual pnt written in output file
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if str(f.minPntCut) != str(ffidMin) or str(f.maxPntCut) != str(ffidMax):
                                                    # @ Get Source Start Point and Endpoint for New FFID
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'
                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + "No Traces Found ;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f

                                elif int(minPnt) >= int(vSegSpStart) and int(maxPnt) <= int(vSegSpEnd):
                                    print(
                                        "3 >> Segmnent Processing min pnt greater than eqaul to Start Match AND max point less than equal to end point")
                                    print("3 >> SegStart -- MinPnt -- MaxPnt --  SegEnd")
                                    # Code for Polycut Testing
                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}

                                    if vPointType == 'FFID':
                                        clslog_log.writeLine(
                                            "\n ffidMin: " + str(ffidMin) + " , ffidMax : " + str(ffidMax))
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)

                                    else:
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        if f.tr_written > 0:
                                            # Rename file with actual pnt written in output file
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if str(f.minPntCut) != str(ffidMin) or str(f.maxPntCut) != str(ffidMax):
                                                    # @ Get Source Start Point and Endpoint for New FFID
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + "No Traces Found ;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f
                                elif int(minPnt) >= int(vSegSpStart) and int(maxPnt) == int(vSegSpEnd):
                                    print(
                                        "4 >> Segmnent Processing min pnt greater that equal to Start Match AND max point equal end point")
                                    print("4 >> SegStart -- MinPnt -- MaxPnt ==  SegEnd")

                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}

                                    if vPointType == 'FFID':
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)
                                    else:
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            # if vPointType == 'FFID':
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        # Rename file with actual pnt written in output file
                                        if f.tr_written > 0:
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if str(f.minPntCut) != str(ffidMin) or str(f.maxPntCut) != str(ffidMax):
                                                    # @ Get Source Start Point and Endpoint for New FFID
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'
                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                os.rename(dstFileName, newfileName)
                                                dstFileName = newfileName
                                                d.remark = str(
                                                    d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut
                                        else:
                                            d.error = str(d.error) + "No Traces Found;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f
                                # For overlap Range with segment Start Point
                                elif (int(vSegSpStart) >= int(maxPnt) and int(maxPnt) >= int(vSegSpEnd)) and (
                                        int(minPnt) < int(vSegSpStart)):
                                    print(
                                        "5 >> Segmnent Processing min pnt less than to Start  AND max point less then equal to than end point, Overlap Range at start Point")
                                    print("5 >> MinPnt -- SegStart -- MaxPnt --  SegEnd")
                                    # Code for Polycut Testing

                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}
                                    if vPointType == 'FFID':
                                        maxPnt = v['min_point_start']
                                        ffidMin = v[
                                            'max_ffid_start']  # Set ffid start point sengment as  min point for clipping
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)
                                    else:
                                        maxPnt = v[
                                            'min_point_start']  # Set sp_start point sengment as  min point for clipping
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            # if vPointType == 'FFID':
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        # Rename file with actual pnt written in output file
                                        if f.tr_written > 0:
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if str(f.minPntCut) != str(ffidMin) or str(f.maxPntCut) != str(ffidMax):
                                                    # @ Get Source Start Point and Endpoint for New FFID
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'
                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + "No Traces Found;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f

                                # For overlap Range with segment End Point
                                elif (int(vSegSpStart) <= int(minPnt) and int(minPnt) <= int(vSegSpEnd)) and (
                                        int(maxPnt) > int(vSegSpEnd)):
                                    print(
                                        "6 >> Segmnent Processing min pnt greater that equal to Start  AND max point greater than end point, Overlap Range at End Point")
                                    print("6 >> SegStart -- MinPnt -- SegEnd -- MaxPnt")
                                    # Code for Polycut Testing
                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}

                                    if vPointType == 'FFID':
                                        maxPnt = v[
                                            'max_point_end']  # Set sp_end point sengment as  max point for clipping
                                        ffidMax = v[
                                            'max_ffid_end']  # Set ffid end point sengment as  max point for clipping
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)
                                    else:
                                        maxPnt = v[
                                            'max_point_end']  # Set sp_end point sengment as  max point for clipping
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            # if vPointType == 'FFID':
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        # Rename file with actual pnt written in output file
                                        if f.tr_written > 0:
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if (str(f.minPntCut) != str(ffidMin)) or str(f.maxPntCut) != str(
                                                        ffidMax):
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + "No Traces Found;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f

                                # For segment in overlap or inside min max point
                                elif int(minPnt) <= int(vSegSpStart) and int(vSegSpEnd) <= int(maxPnt):
                                    print(
                                        "7 >> Segmnent Processing min pnt greater that equal to Start  AND max point greater than end point, Overlap Range at End Point")
                                    print("7 >> MinPnt -- SegStart --- SegEnd -- MaxPnt")
                                    # Code for Polycut Testing
                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    f.read_nb_samples_from_binary = True
                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}

                                    if vPointType == 'FFID':
                                        maxPnt = v[
                                            'max_point_end']  # Set sp_end point sengment as  max point for clipping
                                        ffidMax = v[
                                            'max_ffid_end']  # Set ffid end point sengment as  max point for clipping
                                        minPnt = v[
                                            'min_point_start']  # Set sp_end point sengment as  max point for clipping
                                        ffidMin = v[
                                            'min_ffid_start']  # Set ffid end point sengment as  max point for clipping
                                        f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)], dstFileName)
                                    else:
                                        maxPnt = v[
                                            'max_point_end']  # Set sp_end point sengment as  max point for clipping
                                        minPnt = v[
                                            'min_point_start']  # Set sp_end point sengment as  max point for clipping
                                        f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)

                                    if f.fileReadingStatus == 'UNSUCCESS':
                                        if 'Finding Trace number value 0' in f.fileReadingError:
                                            constants_sgy.trace_header['test_raj'] = {"pos": 8, "type": vheadType,
                                                                                      "human_name": "raj"}
                                            spStartAsFfidFlg, ffidMin, ffidMax = DBUtilities.GetFFIdpntsFromMinMax_SqlDB(
                                                mssql_conn, seg_id,
                                                str(minPnt),
                                                str(maxPnt), vSectName4SubFldr, vSeis_file_id)
                                            # if vPointType == 'FFID':
                                            if (ffidMin == None or ffidMin == '') or (ffidMax == None or ffidMax == ''):
                                                d.error = str(d.error) + 'Unable to determine respective FFID points.;'
                                            else:
                                                f.polycut("test_raj", [range(int(ffidMin), int(ffidMax) + 1)],
                                                          dstFileName)
                                                d.remark = str(
                                                    d.remark) + 'SourcePoint value found 0 so respective FFID points are extracted.;'
                                                readByFFIDFlg = True

                                    if f.fileReadingStatus == 'SUCCESS':
                                        # Rename file with actual pnt written in output file
                                        if f.tr_written > 0:
                                            if vPointType == 'FFID' or readByFFIDFlg == True:
                                                if str(f.minPntCut) != str(ffidMin) or str(f.maxPntCut) != str(ffidMax):
                                                    # @ Get Start Point and Endpoint for New FFID
                                                    updStPnt, updEndPnt = DBUtilities.GetPntStartOfRespectiveFFID(
                                                        mssql_conn, seg_id, str(f.minPntCut), str(f.maxPntCut))
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(updStPnt) + '_' + str(
                                                        updEndPnt) + '_' + vPointType + '.sgy'

                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'
                                            else:
                                                if str(f.minPntCut) != str(minPnt) or str(f.maxPntCut) != str(maxPnt):
                                                    newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                            '').replace(
                                                        '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                        f.maxPntCut) + '_' + vPointType + '.sgy'
                                                    os.rename(dstFileName, newfileName)
                                                    dstFileName = newfileName
                                                    d.remark = str(
                                                        d.remark) + 'Point range is mismatch with actual point range;'

                                            d.pntRangeMin = minPnt
                                            d.pntRangeMax = maxPnt
                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + "No Traces Found;"
                                    else:
                                        d.error = str(d.error) + f.fileReadingError + ';'

                                    del f
                                else:
                                    d.remark = str(d.remark) + 'Not Matched Data;'

                                clslog.WriteCsvLineUsingCls(clsSegDet=d)

                    else:
                        # Processing 3D Points.....................
                        print("3D POINT Data")
                        if seg_id in dict_gis_st_end.keys():
                            varGisStEndPnt = dict_gis_st_end[seg_id]
                            vSegSpStart = varGisStEndPnt['g_seg_stPnt']  # postgis data
                            vSegSpEnd = varGisStEndPnt['g_seg_endPnt']  # postgis data
                        else:
                            d.error = str(d.error) + 'Start End Point Missing in PostGIS Datatabase.;'
                            continue

                        # Get Range of points from clip portion
                        # listPntRangeInside3d = DBUtilities.Get_ListOfPointRangeInsideInPolygonUsingSegID(pPgSqlConn=pgsql_conn,
                        #                                                                                pSegID=seg_id,
                        #                                                                                pWktPolyGon=wkt,
                        #                                                                                pSegStPntNo=vSegSpStart,
                        #                                                                                pSegEndPntNo=vSegSpEnd,
                        #                                                                                pSrid='4326')
                        listPntRangeInside3d = DBUtilities.Get_ListOfPointRangeInsideInPolygonUsingSegID(
                            pPgSqlConn=pgsql_conn,
                            pSegID=seg_id,
                            pWktPolyGon=wkt,
                            pSegStPntNo=vSegSpStart,
                            pSegEndPntNo=vSegSpEnd,
                            pBlockNumber=concession_nb,
                            pPolygonType=pPolygon_type,
                            pSrid='4326')

                        if len(listPntRangeInside3d) > 0:
                            for pntrange in listPntRangeInside3d:

                                str2 = str(pntrange)
                                str3 = (str2.rstrip(')')).lstrip('(')
                                lstRangPntNum = str3.split(',')

                                str3 = lstRangPntNum[0]
                                minPnt = str3.rstrip().lstrip()

                                str4 = lstRangPntNum[1]
                                maxPnt = str4.rstrip().lstrip()

                                if int(minPnt) == int(vSegSpStart) and int(maxPnt) == int(vSegSpEnd):
                                    print("3D >> 1 >> Segmnent Processing for Start and End point matched")
                                    # Just Copy The File

                                    dPath = Utilities.Create_Dir_Structure(fldrNameBlkNoWKt, vProcName4Fldr,
                                                                           vProcSetType4SubFldr,
                                                                           vSectName4SubFldr, output_path, vType2dOr3d)

                                    dstFileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy', '').replace('.segy',
                                                                                                                '') + '_' + str(
                                        minPnt) + '_' + str(
                                        maxPnt) + '_' + vPointType + '.sgy'

                                    copyfile(srcFileName, dstFileName)

                                    d.destPath = dstFileName

                                else:
                                    print("3D >> 2 >> Clipped Segmnent Processing...")
                                    dPath = Utilities.Create_Dir_Structure(fldrNameBlkNoWKt, vProcName4Fldr,
                                                                           vProcSetType4SubFldr, vSectName4SubFldr,
                                                                           output_path,
                                                                           vType2dOr3d)

                                    dstFileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy', '') + '_' + str(
                                        minPnt) + '_' + str(
                                        maxPnt) + '_' + vPointType + '.sgy'

                                    try:
                                        f = segy.Segy(srcFileName)
                                    except:
                                        d.error = str(d.error) + ';' + "Error Reading in SegY File."
                                        clslog.WriteCsvLineUsingCls(clsSegDet=d)
                                        continue

                                    # constants_sgy.trace_header['test_raj'] = {"pos": 20, "type": "N4", "human_name": "raj"}
                                    constants_sgy.trace_header['test_raj'] = {"pos": vHeadPos, "type": vheadType,
                                                                              "human_name": "raj"}

                                    f.polycut("test_raj", [range(int(minPnt), int(maxPnt) + 1)], dstFileName)
                                    if f.fileReadingStatus == 'SUCCESS':
                                        if f.tr_written > 0:
                                            # Rename file with actual pnt written in output file
                                            if str(f.minPntCut) != minPnt or str(f.maxPntCut) != maxPnt:
                                                newfileName = dPath + '/' + str(vOrgnlFileName).replace('.sgy',
                                                                                                        '').replace(
                                                    '.segy', '') + '_' + str(f.minPntCut) + '_' + str(
                                                    f.maxPntCut) + '_' + vPointType + '.sgy'
                                                os.rename(dstFileName, newfileName)
                                                # d.remark = str(d.remark) + ';' + 'Point range is mismatch with actual point range.'

                                            d.destPath = dstFileName
                                            d.fMinPnt = f.minPntCut
                                            d.fMaxPnt = f.maxPntCut

                                        else:
                                            d.error = str(d.error) + ';' + "No Traces Found."
                                    else:
                                        d.error = str(d.error) + ';' + f.fileReadingError

                                    d.insidePntRangeMin = d.pntRangeMin = minPnt
                                    d.insidePntRangeMax = d.pntRangeMax = maxPnt
                                    d.intSectPnt = str(minPnt) + "-" + str(maxPnt)
                                    d.ffidMin = 'NA'
                                    d.ffidMax = 'NA'

                                    del f

                                clslog.WriteCsvLineUsingCls(clsSegDet=d)
                        else:
                            d.remark = str(d.remark) + ';' + 'No point found inside polygon.'
                            clslog.WriteCsvLineUsingCls(clsSegDet=d)

                else:
                    d.remark = str(d.remark) + 'Dataset id not Found in list;'
                    clslog.WriteCsvLineUsingCls(clsSegDet=d)

                clsPgSqlConnObj.pgsql_connect_close(pgsql_conn)

        print("\nWait...Preparing Data Sheet will take some time ..............................................")
        DBUtilities.WriteDataSheet(mssql_conn, dict_geom_new, dict_ppdm, output_path, segment_id_set, fldrNameBlkNoWKt)
        clslog.writeLine("\n Summary :")
        clslog.writeLine("\nTotal Segment found PostGis DB (Inside Area) :,%s" % str(len(dict_geom)))
        clslog.writeLine("\nTotal Segment Record Processed from PPDM:,%s" % str(len(dict_ppdm)))
        clslog.writeLine("\nProcess End Time :, %s" % strftime("%Y-%m-%d %H:%M:%S", localtime()))
        clslog.closefile()
        clslog_log.closefile()




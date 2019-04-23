import os
import sys
import pymssql
import argparse
import shutil
import time
from time import gmtime, strftime

def Get_Dir_StructureMain(pBlockNoOrWkt , output_path):
    # To create Main Directore Folder ,

    oPath = ''
    fldrNameForBlkWkt =''

    if pBlockNoOrWkt.upper() == 'WKT':
        pBlockNoOrWktDir = 'AOI' + '_' + str(time.strftime("%d_%m_%y"))
        fldrNameForBlkWkt = pBlockNoOrWktDir
        oPath = output_path + fldrNameForBlkWkt
    else:
        fldrNameForBlkWkt = 'BLOCK' + '_' + pBlockNoOrWkt + '_'+ str(time.strftime("%d_%m_%y"))
        oPath = output_path + fldrNameForBlkWkt

    return oPath, fldrNameForBlkWkt

def Create_Dir_StructureMain(pBlockNoOrWkt , output_path):
    # To create Main Directore Folder ,

    oPath = ''
    fldrNameForBlkWkt =''

    if pBlockNoOrWkt.upper() == 'WKT':
        pBlockNoOrWktDir = 'AOI05' + '_' + str(time.strftime("%d_%m_%y"))
        fldrNameForBlkWkt = pBlockNoOrWktDir
        oPath = output_path + fldrNameForBlkWkt
    else:
        fldrNameForBlkWkt = 'BLOCK' + '_' + pBlockNoOrWkt + '_'+ str(time.strftime("%d_%m_%y"))
        oPath = output_path + fldrNameForBlkWkt

    if not os.path.exists(oPath):
        os.mkdir(oPath)
    else:
        print "\nFolder %s Exists, Do you want delete the folder ? \n Enter Y/N :" % oPath
        delete_folder_input = raw_input()

        while delete_folder_input.upper() not in ('Y' , 'N', 'YES', 'NO'):
            print "\nWrong Input :"
            print "Folder %s Exists, Do you want delete the folder ? \n Enter Y/N :" % oPath
            delete_folder_input = raw_input()

        if delete_folder_input.upper() == 'Y' or delete_folder_input.upper() == 'YES':
            print "\nPlease Confirm Yes/No :"
            confirn_delete_folder = raw_input()
            while confirn_delete_folder.upper() not in ('Y', 'N', 'YES', 'NO'):
                print "\nWrong Input :"
                print "Please Confirm to delete the folder %s ? \n Enter Y/N :" % oPath
                confirn_delete_folder = raw_input()

            if confirn_delete_folder.upper() == 'Y' or confirn_delete_folder.upper() == 'YES':
                shutil.rmtree(oPath)
                os.mkdir(oPath)
        else:
            print "\nDo you want to continue with another folder  name ? \n Enter Y/N :"
            new_folder_input1 = raw_input()
            if new_folder_input1.upper() == 'Y' or new_folder_input1.upper() == 'YES':
                loooCnt = 1
                fldCreatFlg = False
                while fldCreatFlg == False:
                    if loooCnt == 1:
                        print "\nEnter folder suffix : "
                        folder_name_ext = raw_input()
                    else:
                        print "\nPlease, Enter another folder suffix name : "
                        folder_name_ext = raw_input()

                    fldrNameForBlkWkt = fldrNameForBlkWkt + '_' + folder_name_ext
                    oPath = output_path + fldrNameForBlkWkt
                    if not os.path.exists(oPath):
                        os.mkdir(oPath)
                        fldCreatFlg = True
                    else:
                        print('Directory Already Exists .')

    return fldrNameForBlkWkt;

def Create_Dir_Structure(pBlockNoOrWkt, pProcName4Fldr, pProcSetType4SubFldr, pSectName4SubFldr=None , Output_path=None , pType2dOr3d= None ):

    oPath =''

    if pBlockNoOrWkt.upper() ==  'WKT':
        pBlockNoOrWktDir = 'AOI' + '_' + str(time.strftime("%d_%m_%y"))
        oPath = output_path + pBlockNoOrWktDir.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)
    else:
        oPath = Output_path + pBlockNoOrWkt.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)

    if pType2dOr3d != None and pType2dOr3d != '':
        pType2dOr3d = pType2dOr3d.replace('/', '_')
        oPath = oPath + '/' + pType2dOr3d.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)

    if pProcName4Fldr != None and pProcName4Fldr != '':
        pProcName4Fldr = pProcName4Fldr.replace('/', '_')
        oPath = oPath + '/' + pProcName4Fldr.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)

    if pProcSetType4SubFldr != None and pProcSetType4SubFldr != '':
        pProcSetType4SubFldr = pProcSetType4SubFldr.replace('/', '_')
        oPath = oPath + '/' + pProcSetType4SubFldr.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)

    if pSectName4SubFldr != None and pSectName4SubFldr != '':
        pSectName4SubFldr = pSectName4SubFldr.replace('/', '_')
        oPath = oPath + '/' + pSectName4SubFldr.replace(' ','_')
        if not os.path.exists(oPath):
            os.mkdir(oPath)

    oPath = str(oPath).replace(' ', '_')
    return oPath

def createRanges(lst,pThreshold):
    s = e = None
    r = []
    for i in sorted(lst):
        if s is None:
            s = e = i
        #elif i == e or i == e + 1:
        elif i == e or i <= e + pThreshold:
            e = i
        else:
            r.append((s, e))
            s = e = i
    if s is not None:
        r.append((s, e))
    return r


def GetListOfNumberFileFileNameList(pListOfNames = None , pFileNameSuffix= None , pFileNameExt= None ):
    """
        Returns the FFID number of list  after parsing the list of file name
        Args:
        pListOfNames (list): List of file name     (/home/raj/OGDR/FLD_87-03___0001_2.segd , /home/raj/OGDR/FLD_87-03___0001_3.segd, .......)
        pFileNameSuffix (str)  :  file name sufffix which will replace to '' FLD_87-03___0001
        pFileNameExt (str)  :  file name extension which will replace to ''  segd
        Returns:
            list of FFID Numbes : list of numbers (2,3,.......)
        """
    lstOfNumbers = []

    for fname in pListOfNames:
        head, tail = os.path.split(fname)
        tail = tail.replace(pFileNameSuffix,'')
        tail = tail.replace( pFileNameExt.lower(), '')
        tail = tail.replace(pFileNameExt.upper(), '')
        tail = tail.lstrip('0')
        tail = tail.strip()
        try:
            if tail != None and tail != '':
                lstOfNumbers.append(int(tail))

        except:
            print tail
            print("Unexpected error:", sys.exc_info()[0])

    return lstOfNumbers

def GetRangeStringfromListOfRage(pListOfNum = None):
    """
    Returns the string of ranges from  list of Number
    Args:
    pListOfNum (list): List of integers (numbers)    {1,2,....20,35,...,40}

    Returns:
        string : strListOfRanges Returns the string of ranges (1,20)(35-20)....
    """

    # sorted numbers in ascending order
    lstNumber1 = sorted(pListOfNum, key=int)

    #Create ranges of list numbers
    listOfRange = createRanges(lstNumber1,1)

    strListOfRanges=''
    for i in listOfRange:
        strListOfRanges = strListOfRanges + str(i).replace(',','-')

    return strListOfRanges

def N_GetType2dOr3d(vProcSetType4SubFldr, vProdTyp):

    vType2dOr3d = ''
    if '3D' in str(vProdTyp).upper():
     if 'FIELD' in str(vProcSetType4SubFldr).upper():
         vType2dOr3d = '3D_SEISMIC_FIELD_DATA'
     else:
         vType2dOr3d = '3D_SEISMIC_PROCESS_DATA'

    elif '2D' in str(vProdTyp).upper():
     if 'FIELD' in str(vProcSetType4SubFldr).upper():
         vType2dOr3d = '2D_SEISMIC_FIELD_DATA'
     else:
         vType2dOr3d = '2D_SEISMIC_PROCESS_DATA'
    else:
     vType2dOr3d = 'Error : Product Type not like 2D or 3D ;'

    return vType2dOr3d


def N_GetHeaderFormat(clslog_log, dict_segyFormat, headFormat, seisFileName ,seg_id ):

    try:
     vHeaderInfo = dict_segyFormat[headFormat]

     ILINE_FORMAT = vHeaderInfo['ILINE_FORMAT']
     POINT_POSITION = vHeaderInfo['POINT_POSITION']
     POINT_MODULO = vHeaderInfo['POINT_MODULO']
     XLINE_FORMAT = vHeaderInfo['XLINE_FORMAT']
     POINT_FORMAT = vHeaderInfo['POINT_FORMAT']
     
     clslog_log.writeLine("\n        headFormat: " + headFormat + ", ILINE_FORMAT: " + ILINE_FORMAT + " ,POINT_POSITION: " + POINT_POSITION  + " ,POINT_MODULO: " + POINT_MODULO + " ,XLINE_FORMAT: " + XLINE_FORMAT + " ,POINT_FORMAT: " + POINT_FORMAT )
     # if (ILINE_FORMAT != '' and ILINE_FORMAT != None) and POINT_MODULO != '' and POINT_MODULO != None:
     #     print("Multiple Header Information found for header % found in Header CSV file", headFormat)
     #     d.error = str(d.error) + ';' + 'Information for header '+ str(headFormat) +' not found in Header CSV file.'
     #     clslog.WriteCsvLineUsingCls(clsSegDet=d)
     #     continue
     vHeadPos = '';
     vheadType = '';
     errorFlg= False;

     if ILINE_FORMAT != '' and ILINE_FORMAT != None:
         vheadType = 'N' + str(ILINE_FORMAT)
         if POINT_POSITION == '' or POINT_POSITION == None:
             clslog_log.writeLine("Skipping segment " + seg_id + " having file name=" + str(
                 seisFileName) + " due to wrong for header info>> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_POSITION:" + POINT_POSITION)
             clslog_log.writeLine("POINT_POSITION is null or none for header format:" + headFormat)
             clslog_log.writeLine("Check Segment CSV header information file ")
             clslog_log.writeLine("-------------------------------------------------------------------------------------")
             errorFlg = True
             # continue
         else:
             vHeadPos = int(POINT_POSITION) - 1
     elif POINT_MODULO != '' and POINT_MODULO != None:
         vheadType = 'N' + str(POINT_MODULO)
         if XLINE_FORMAT == '' or XLINE_FORMAT == None:
             clslog_log.writeLine("\nSkipping segment " + seg_id + " having file name=" + str(
                 seisFileName) + " due to wrong for header info>> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_MODULO:" + POINT_MODULO)
             clslog_log.writeLine("\nXLINE_FORMAT is null or none for header format:" + headFormat)
             clslog_log.writeLine("\nCheck Segment CSV header information file ")
             clslog_log.writeLine("-------------------------------------------------------------------------------------")
             errorFlg = True
             # continue
         else:
             vHeadPos = int(XLINE_FORMAT) - 1
     elif (ILINE_FORMAT == '' or ILINE_FORMAT == None) and (POINT_MODULO == '' or POINT_MODULO == None) and (POINT_FORMAT != None or POINT_FORMAT != ''):
         vheadType = 'N' + str(POINT_FORMAT)
         vHeadPos = int(POINT_POSITION) - 1

     if vHeadPos == '' or vHeadPos == None or vheadType == '' or vheadType == None:
         print("Error : Information for header %s not found in Header seis_format CSV file", headFormat)
         clslog_log.writeLine( '\nInformation for header ' + str(headFormat) + ' not found in Header CSV file;')
         errorFlg = True


     clslog_log.writeLine("\n        vHeadPos : " + str(vHeadPos) + " , vheadType : " + str(vheadType))   

    except KeyError:
     print("Error : Information for header " + headFormat + " not found in Header CSV file")
     clslog_log.writeLine("\nSkipping segment " + seg_id + " having file name=" + str(seisFileName))
     clslog_log.writeLine("\nInformation for header format:" + headFormat + " not found in Header seis_format.csv file.")
     d.error = str(d.error) + 'Information for header is not found in Header CSV seis_format.csv file;'
     clslog_log.writeLine("\n-------------------------------------------------------------------------------------")
     errorFlg = True
    except:
     print("Error : Header Information is not sufficient or wrong for header format " + headFormat + " in seis_format.csv file")
     clslog_log.writeLine("\nSkipping segment " + seg_id + " having file name = " + str(seisFileName))
     clslog_log.writeLine("\nDue to wrong for header info >> headFormat:" + headFormat + ", ILINE_FORMAT:" + ILINE_FORMAT + " ,POINT_MODULO:" + POINT_MODULO)
     clslog_log.writeLine("\n-------------------------------------------------------------------------------------")
     errorFlg = True

    return vHeadPos, vheadType ,errorFlg



def N_GetFFidFromSpPoints(ffidMin, ffidMax, overlapRngMin, overlapRngMax,spMin , spMax , pIsSPreverseFlg , pIsReverseFlgFFID):
    rngMin = 0;
    rngMax = 0;

    if spMin == overlapRngMin and spMax == overlapRngMax:
     rngMin = ffidMin
     rngMax = ffidMax
    else:

     # setGisPnts = set(range(overlapRngMin, overlapRngMax, 1))
     # setSeisFile = set(range(spMin, spMax, 1))
     # setr = setGisPnts.intersection(setSeisFile)

     ffidDiff = ffidMax - ffidMin
     spdiff = spMax - spMin

     ratio = ffidDiff / spdiff

     if '.' in str(ratio) :
        aa = []
        aa = str(ratio).split('.')
        pre1 = '0.' + aa[1]
        preFl = float(pre1)
        # if (preFl > 0.9) or (preFl > 0.4 and preFl < 0.5):
        #     ratio = round(ratio, 1)

        if (preFl > 0.9 and preFl < 1):
            ratio = 1
        elif (preFl > 0.4 and preFl < 0.5):
            ratio = round(ratio, 1)
        else:
            ratio = round(ratio, 1)


     if pIsSPreverseFlg == True and pIsReverseFlgFFID == False :
         # rngMin = ((spMax - int(overlapRngMax)) * ratio) + ffidMin
         # rngMax = ffidMax - ((int(overlapRngMin) - spMin) * ratio)

         rngMin = ( ( float(spMax) - float(overlapRngMax) ) * float(ratio)  ) + float( ffidMin)
         rngMax = float(ffidMax) - ( (float(overlapRngMin) - float(spMin)  ) * float(ratio) )

         rngMin = int(rngMin)
         rngMax = int(rngMax)

         if spMin == overlapRngMin:
             rngMax = ffidMax

         if spMax == overlapRngMax:
             rngMin = ffidMin

     else:
         # rngMin = ((int(overlapRngMin) - spMin) * ratio) + ffidMin
         # rngMax = ffidMax - ((spMax - int(overlapRngMax)) * ratio)

         rngMin = ( (float(overlapRngMin) - float(spMin) ) * float(ratio)  ) + float( ffidMin)
         rngMax = float(ffidMax) - ( ( float(spMax) - float(overlapRngMax)) * float(ratio) )

         rngMin = int(rngMin)
         rngMax = int(rngMax)

         if spMin == overlapRngMin:
             rngMin = ffidMin

         if spMax == overlapRngMax:
             rngMax = ffidMax

    return rngMin , rngMax

import csv
import os

class SegyCSVFormatError(Exception):
    pass


class cls_SegYCSVFormat:
    def __init__(self,data=None):
        self.data= data

    def fromCsvFile(cls, filename):
        try:
            if os.path.isfile(filename) :
                fileH = open(filename, 'r')
                if not fileH:
                    raise SegyCSVFormatError("Error: Invalid or corrupt Seis file format Fil")
                else:
                    fileH.close()
            else:
                print "%sFile Not Found !!" % filename
                return False

        except IOError:
            print "No such file or directory: '%s'" % filename
            return False

        dict_SegyFormat = {}
        with open(filename, 'r') as csvfile:
            csvdata = csv.DictReader(csvfile)
            print csvdata.fieldnames


           #csvdata = csv.DictReader(filename, delimiter=",", dialect=csv.excel, quoting=csv.QUOTE_NONE)
            if not 'NAME' in csvdata.fieldnames:
                print('Error:- NAME column NOT found in csv file')
            else:
                print('NAME column found in csv file')
                for each_row in csvdata:
                    #for f in csvdata.fieldnames:
                    #    print f
                    #    print (each_row[f])
                    data_dict = zip(csvdata.fieldnames, each_row )
                    if not each_row['NAME'] in dict_SegyFormat.keys():
                        dict_SegyFormat[each_row['NAME']]= each_row
                    else:
                        print('Error> Duplicate Name %s found in CSV', each_row['NAME'])


        return  dict_SegyFormat

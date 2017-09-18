"""-----------------------------------------------------------------------------
Name: sanitize.py
Purpose: Replaces values in a field of a feature class with another value.
Description: Replaces a specific value within an attribute field with a user
        defined value.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: April, 2017
Modified:
Copyright: Esri
License:
-----------------------------------------------------------------------------"""

import os
import sys
import platform
import itertools

import arcpy
from arcpy import env
from arcpy import da
import numpy as np
import pandas as pd

#--------------------------------------------------------------------------
class FunctionError(Exception):
    """ raised when a function fails to run """
    pass
#--------------------------------------------------------------------------
def trace():
    """
        trace finds the line, the filename
        and error message and returns it
        to the user
    """
    import traceback
    tb = sys.exc_info()[2]
    tbinfo = traceback.format_tb(tb)[0]
    # script name + line number
    line = tbinfo.split(", ")[1]
    # Get Python syntax error
    #
    synerror = traceback.format_exc().splitlines()[-1]
    return line, __file__, synerror
#--------------------------------------------------------------------------
def grouper_it(n, iterable):
    """
    creates chunks of cursor row objects to make the memory
    footprint more manageable
    """
    it = iter(iterable)
    while True:
        chunk_it = itertools.islice(it, n)
        try:
            first_el = next(chunk_it)
        except StopIteration:
            return
        yield itertools.chain((first_el,), chunk_it)
#--------------------------------------------------------------------------
def calc_chunk_size():
    """determines the chunk size based on 32 vs 64-bit python"""
    try:
        if platform.architecture()[0].lower() == "32bit":
            return 50000
        else:
            return 500000
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                    "function": "calc_chunk_size",
                    "line": line,
                    "filename": __file__,
                    "synerror": synerror,
                    "arc" : str(arcpy.GetMessages(2))
                }
        )
#--------------------------------------------------------------------------
def replace_values(fc,
                  fields="*",
                  oid_field=None,
                  find_value=None,
                  replace_value=0,
                  where_clause=None):
    """updates a set of rows in chunks

    """
    try:
        if fields is None or \
           isinstance(fields, list) == False or \
           fields == "*":
            fields = [field.name for field in arcpy.ListFields(fc) \
                      if field.type not in ('Geometry', 'Blob', 'Raster')]
        if oid_field is None:
            oid_field = arcpy.Describe(fc).OIDFieldName
        chunk_size = calc_chunk_size()
        if oid_field not in fields:
            fields.append(oid_field)
        with da.SearchCursor(fc, fields,
                             where_clause=where_clause) as cursor:
            search_fields = [field for field in cursor.fields if field != oid_field]
            for group in grouper_it(chunk_size, cursor):
                df = pd.DataFrame.from_records(group, columns=cursor.fields)
                for field in search_fields:
                    if  find_value is None or \
                        str(find_value).lower() == "none" or \
                        str(find_value).lower().strip() == "":
                        df.loc[df[field].isnull(), field] = replace_value
                    else:
                        df.loc[df[field] == find_value, field] = replace_value
                    del field
                array = df.to_records(index=False)
                da.ExtendTable(fc, oid_field,
                               array, oid_field,
                               False)
                del array
                del df
        return fc
    except:
        line, filename, synerror = trace()
        raise FunctionError(
            {
                "function": "replace_values",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
            }
        )
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        table = argv[0]
        fields = str(argv[1]).split(';')#['building']#
        find_value = argv[2]#arcpy.GetParameter(2)#'yes'#'bank'#
        replace_value = argv[3]#None#
        where_clause = argv[4]#None#
        in_place = str(argv[5]).lower() == "true"#argv[5]
        #  Local Variables
        #
        scratchGDB = env.scratchGDB
        scratchFolder = env.scratchFolder
        in_place_fc = os.path.join(scratchGDB, os.path.basename(table)+"_copy")
        isFC = False
        #  Logic
        #
        # Determine if table or feature class
        desc = arcpy.Describe(table)
        datasetType = desc.datasetType
        if in_place == False:

            if datasetType == "Table":
                in_place_fc = arcpy.CopyRows_management(table, in_place_fc)[0]
            elif datasetType == "FeatureClass":
                in_place_fc = arcpy.CopyFeatures_management(table, in_place_fc)[0]
            else:
                raise Exception("Invalid datatype of " + desc.datasetType)
        else:
            in_place_fc = table
        del desc
        #perform the replace
        table = replace_values(fc=in_place_fc,
                              fields=fields,
                              oid_field=None,
                              find_value=find_value,
                              replace_value=replace_value,
                              where_clause=where_clause)
        # return results
        arcpy.SetParameterAsText(6, table)


    except arcpy.ExecuteError:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
        arcpy.AddError("ArcPy Error Message: %s" % arcpy.GetMessages(2))
    except FunctionError as f_e:
        messages = f_e.args[0]
        arcpy.AddError("error in function: %s" % messages["function"])
        arcpy.AddError("error on line: %s" % messages["line"])
        arcpy.AddError("error in file name: %s" % messages["filename"])
        arcpy.AddError("with error message: %s" % messages["synerror"])
        arcpy.AddError("ArcPy Error Message: %s" % messages["arc"])
    except:
        line, filename, synerror = trace()
        arcpy.AddError("error on line: %s" % line)
        arcpy.AddError("error in file name: %s" % filename)
        arcpy.AddError("with error message: %s" % synerror)
#--------------------------------------------------------------------------
if __name__ == "__main__":
    env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)
"""-----------------------------------------------------------------------------
Name: basic_table_tracking.py
Purpose: Compares datasets based on an excel table.
Description: Compares two datasets that have the same schema based on a list of
        features within feature classes. Features and attributes that are being
        compared are taken from an excel spreadsheet.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: Fall 2016
Modified: April, 2017
Copyright: Esri
License:
-----------------------------------------------------------------------------"""

from __future__ import division
from __future__ import absolute_import
from __future__ import print_function
import os
import csv
import sys
import shutil
import pandas as pd
import numpy as np
import arcpy
import xlrd
import xlwt
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping

def assemble_query(xlsx,
                   sheet_name="FGCM Metrics",
                   fcField="Feature Class",
                   subtypesField="FCSubtype",
                   fcodeField="F_CODE",
                   fcodedesc="FCSubtype_Description",
                   queryField="Query"):
    """converts a given xlsx sheet into a dictionary where
    the first column is the Key and everything else is the
    value pair"""
    queries = {}
    xl_workbook = xlrd.open_workbook(xlsx)
    sheet = xl_workbook.sheet_by_name(sheet_name)
    col_names = [sheet.cell(0,col).value for col in range(sheet.ncols)]
    for nrow in range(sheet.nrows):
        if nrow > 0:
            val = {}
            fc = sheet.cell(nrow,col_names.index(fcField)).value
            if fc not in queries:
                queries[fc] = []
            subtype = int(sheet.cell(nrow,col_names.index(subtypesField)).value)
            fcodeval = sheet.cell(nrow,col_names.index(fcodeField)).value
            add_q = sheet.cell(nrow,col_names.index(queryField)).value
            FCSubtype_Description = sheet.cell(nrow,col_names.index(fcodedesc)).value
            query = "{sfield} = {sval} and {fcodefld} = '{fcodevalue}'".format(sfield=subtypesField,
                                                                               sval=int(subtype),
                                                                               fcodefld=fcodeField,
                                                                               fcodevalue=fcodeval)
            if len(add_q.strip()) > 0:
                query += " and %s" % add_q
            val['F_CODE'] = fcodeval
            val['FCSubtype_Description'] = FCSubtype_Description
            val['SUBTYPE'] = subtype
            val['query'] = query
            queries[fc].append(val)
    return queries

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
def summary_values(fc,
                   sr=None,
                   sql=None,
                   area_units="SQUAREKILOMETERS",
                   length_units="KILOMETERS"):
    """
    Inputs:
     fc: table
     sr: spatial reference object
     sql: where clause
     area_units: area units
     length_units: length units
    output:
      returns list of Area, Perimter, Length
    """
    #Area, Perimeter/Length
    calculations = [0,0]
    desc = arcpy.Describe(fc)
    if hasattr(desc, 'shapeType') and \
       desc.shapeType in ('Polygon', 'Polyline'):
        with da.SearchCursor(fc,
                             field_names=['SHAPE@'],
                             where_clause=sql,
                             spatial_reference=sr) as rows:
            for row in rows:
                geom = row[0]
                if isinstance(geom, arcpy.Polygon):
                    calculations[0] += round(geom.getArea(units=area_units), 5)
                    calculations[1] += round(geom.getLength(units=length_units), 5)
                    #calculations[2] += 1
                elif isinstance(geom, arcpy.Polyline):
                    calculations[1] += round(geom.getLength(units=length_units), 5)
                    #calculations[2] += 1
                del geom
                del row
            del rows
        return calculations
    return calculations
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        new_gdb = argv[0]#
        old_gdb = argv[1]#
        lookup_spreadsheet = argv[2]#
        sheet_name = argv[3]
        output_format = str(argv[4]).upper() # EXCEL, CSV, FGDB
        #   Local Variable
        #
        scratchFolder = env.scratchFolder
        csv_file = os.path.join(scratchFolder, "comparison.csv")
        xlsx_file = os.path.join(scratchFolder,"comparison.xls")
        output_gdb = os.path.join(scratchFolder, "comparison.gdb")
        new_fcs = []
        old_fcs = []
        schema_diffs = []
        table_trackers = []
        basic_information = []
        basic_infomation_fields = ['TABLE', 'OLD_COUNT',
                                   'NEW_COUNT', 'FTYPE',
                                   "F_CODE", "FCSubtype_Description", "REMOVE_FIELDS",
                                   "ADDED_FIELDS", "SR_OLD",
                                   'SR_NEW', "ISSUES",
                                   'LENGTH_CHANGE', 'AREA_CHANGE']
        #   Logic
        #
        queries = assemble_query(xlsx=lookup_spreadsheet,
                                 sheet_name=sheet_name)
        if os.path.isdir(output_gdb):
            shutil.rmtree(output_gdb, ignore_errors=True)
        output_gdb = arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(output_gdb),
                                                    out_name=os.path.basename(output_gdb))[0]
        tbl = arcpy.CreateTable_management(out_path=output_gdb, out_name="table_tracker")[0]
        array = np.array([],
                         np.dtype([('_id',np.int32),
                                   ('TABLE', '|S256'),
                                   ('FTYPE', '|S256'),
                                   ('FCSubtype_Description', '|S70'),
                                   ('F_CODE', '|S256'),
                                   ('OLD_COUNT', '|S25'),
                                   ('NEW_COUNT', '|S25'),
                                   ('REMOVE_FIELDS', '|S256'),
                                   ('ADDED_FIELDS', '|S256'),
                                   ('SR_OLD', '|S256'),
                                   ('SR_NEW', '|S256'),
                                   ('ISSUES', '|S1000'),
                                   ('LENGTH_CHANGE', np.float64),
                                   ('AREA_CHANGE', np.float64)]))
        arcpy.da.ExtendTable(tbl,
                             arcpy.Describe(tbl).OIDFieldName,
                             array,
                             "_id")
        del array
        env.workspace = new_gdb
        new_fcs = [fc for fc in arcpy.ListFeatureClasses()]
        env.workspace = old_gdb
        old_fcs = [fc for fc in arcpy.ListFeatureClasses()]
        env.workspace = None
        for fc in queries.keys():
            if fc in new_fcs and \
               fc not in old_fcs:
                basic_information.append(
                                        [fc,
                                         "",
                                         "",
                                         "",
                                         "",
                                         "",
                                         "",
                                         "",
                                         "",
                                         "",
                                         "NO OLDER DATASET TO COMPARE",
                                         "",
                                         "",
                                         ]
                                    )
            elif fc not in new_fcs and \
                 fc in old_fcs:
                basic_information.append(
                    [fc,
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "",
                     "NO NEWER DATASET TO COMPARE",
                     "",
                     "",
                     ]
                )
            elif fc not in new_fcs and \
                 fc not in old_fcs:
                basic_information.append(
                                    [fc,
                                     "",
                                     "",
                                     "",
                                     "",
                                     "",
                                     "",
                                     "",
                                     "",
                                     "",
                                     "DATASET TO COMPARE ARE MISSING",
                                     "",
                                     "",
                                     ]
                )
            else:
                new_fc = os.path.join(new_gdb, fc)
                old_fc = os.path.join(old_gdb, fc)
                for subs in queries[fc]:
                    ISSUES = []
                    query_info = subs
                    f_code = query_info['F_CODE']
                    subtype = query_info['SUBTYPE']
                    FCSubtype_Description = query_info['FCSubtype_Description']
                    sql = subs['query']
                    new_fields = [field.name for field in arcpy.ListFields(new_fc) \
                                  if field.type not in ('OID', 'Geometry')]
                    old_fields = [field.name for field in arcpy.ListFields(old_fc) \
                                  if field.type not in ('OID', 'Geometry')]

                    sr_new = arcpy.Describe(new_fc).spatialReference.factoryCode
                    sr_old = arcpy.Describe(old_fc).spatialReference.factoryCode
                    if len(sql) > 0:
                        oldlyr = arcpy.MakeFeatureLayer_management(old_fc, "old", where_clause=sql)[0]
                        newlyr = arcpy.MakeFeatureLayer_management(new_fc, "new", where_clause=sql)[0]
                        old_cnt = arcpy.GetCount_management(oldlyr)[0]
                        new_cnt = arcpy.GetCount_management(newlyr)[0]
                    else:
                        old_cnt = arcpy.GetCount_management(old_fc)[0]
                        new_cnt = arcpy.GetCount_management(new_fc)[0]
                    if int(old_cnt) > 0:
                        calc_old = summary_values(fc=old_fc, sql=sql)
                    else:
                        calc_old = [0,0]
                    if int(new_cnt) > 0:
                        calc_new = summary_values(fc=new_fc, sql=sql)
                    else:
                        calc_new = [0,0]

                    if int(old_cnt) > int(new_cnt):
                        ISSUES.append("RECORDS DELETED")
                    elif int(old_cnt) < int(new_cnt):
                        ISSUES.append("RECORDS ADDED")
                    if sr_new != sr_old:
                        ISSUES.append("DIFFERENT SPAITAL REFERENCE")
                    if len(set(new_fields) - set(old_fields)) > 0:
                        ISSUES.append("FIELDS ADDED")
                    if len(list(set(new_fields) - set(old_fields))) > 0:
                        ISSUES.append("FIELDS REMOVED")
                    diff_area = round((calc_new[0] - calc_old[0]), 4)
                    diff_per = round((calc_new[1] - calc_old[1]), 4)
                    if diff_area > 0:
                        ISSUES.append("AREA ADDED")
                    elif diff_area < 0:
                        ISSUES.append("AREA REMOVED")
                    if diff_per > 0:
                        ISSUES.append("LENGTH ADDED")
                    elif diff_per < 0:
                        ISSUES.append("LENGTH REMOVED")
                    basic_information.append(
                        [fc,
                         str(old_cnt),
                         str(new_cnt),
                         subtype,
                         f_code,
                         FCSubtype_Description,
                         ",".join(list(set(old_fields) - set(new_fields))),
                         ",".join(list(set(new_fields) - set(old_fields))),
                         str(sr_old),
                         str(sr_new),
                         ",".join(ISSUES),
                         diff_per,
                         diff_area,
                         ]
                    )
            del fc

        if output_format.upper() == "FGDB":
            icur = da.InsertCursor(tbl, basic_infomation_fields)
            for row in basic_information:
                icur.insertRow(row)
                del row
            del icur
            arcpy.SetParameterAsText(5, tbl)
        elif output_format.upper() == "CSV":
            if os.path.isfile(csv_file):
                os.remove(csv_file)
            df = pd.DataFrame(data=basic_information, columns=basic_infomation_fields)
            df.to_csv(path_or_buf=csv_file,
                     columns=basic_infomation_fields, index_label="OID")
            arcpy.SetParameterAsText(5, csv_file)
        else:
            if os.path.isfile(xlsx_file):
                os.remove(xlsx_file)
            wb = xlwt.Workbook()
            sheet = wb.add_sheet("Analysis_Results")
            for j, col in basic_infomation_fields:
                sheet.write(0, j, col)
            for i, row in enumerate(basic_information):
                for j, col in enumerate(row):
                    sheet.write(i+1, j, col)
            wb.save(xlsx_file)
            arcpy.SetParameterAsText(5, xlsx_file)
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
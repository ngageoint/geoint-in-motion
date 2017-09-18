"""-----------------------------------------------------------------------------
Name: spatial_grid_comparison.py
Purpose: Compares corresponding feature classes within different geodatabase.
        This is intended to compare snapshots of the same database taken at two
        different times.
Description: Looks at two geodatabases and compares datasets with the same name.
        The grid is then used compare the differences in the number of
        features in each polygon feature. The cell is then ranked between -5
        to 5 where 5 means many new features were added, and -5 means there
        was a large removal of features in that given area.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor for NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: March, 2017
Modified: April, 2017
Copyright: Esri
License:
-----------------------------------------------------------------------------"""
from __future__ import absolute_import
from __future__ import print_function
from __future__ import division
import os
import sys
import datetime
import pandas as pd
import numpy as np
import arcpy
from arcpy import env
from arcpy import da
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
def calculate_frequency_ranking(array, methods=None):
    """

    Calculates rankings from a numpy array
    expects the columns: FREQUENCY, OLD_FREQUENCY, SCORE, and RANKING
    Input:
     array - numpy ndarray
    Output:
     FGDB table path
    """
    try:
        if methods is None:
            methods = ['POINT']
        tcsv = os.path.join(env.scratchFolder, 'data.csv')
        tcsv_fgdb = os.path.join(env.scratchGDB, 'data_stats')
        df = pd.DataFrame(array,
                          columns=array.dtype.names)
        for m in methods:
            if m == "POINT":
                df.loc[df['OLD_FREQUENCY'] > 0, 'SCORE'] = df['FREQUENCY']/df['OLD_FREQUENCY']
                df.loc[df['OLD_FREQUENCY'] == 0, 'SCORE'] = -1
                df['DIFF'] = df['FREQUENCY'] - df['OLD_FREQUENCY']
                df.loc[(df['SCORE'] >= 0) & (df['SCORE'] <= .5), 'RANKING'] = 1
                df.loc[(df['SCORE'] > 0.5) & (df['SCORE'] <= .75), 'RANKING'] = 2
                df.loc[(df['SCORE'] > 0.75) & (df['SCORE'] <= 1.25), 'RANKING'] = 3
                df.loc[(df['SCORE'] > 1.25) & (df['SCORE'] <= 1.5), 'RANKING'] = 4
                df.loc[df['SCORE' ] > 1.5, 'RANKING'] = 5
                df.loc[(df['SCORE'] == -1) & (df['FREQUENCY'] > 0), 'RANKING'] = 5
                df.loc[(df['SCORE'] == -1) & (df['FREQUENCY'] <= 0), 'RANKING'] = 1
                df.loc[(df['DIFF'] < 0), 'RANKING'] = -1 * df['RANKING']
            elif m == "POLYLINE":
                df['RANKING_LENGTH'] = 0.0
                df['SCORE_LENGTH'] = 0.0
                df['DIFF_LENGTH'] = df['NEW_LENGTH'] - df['OLD_LENGTH']
                df.loc[df['OLD_LENGTH'] > 0, 'SCORE_LENGTH'] = df['NEW_LENGTH']/df['OLD_LENGTH']
                df.loc[df['OLD_LENGTH'] == 0, 'SCORE_LENGTH'] = -1
                df.loc[(df['SCORE_LENGTH'] >= 0) & (df['SCORE_LENGTH'] <= .5), 'RANKING_LENGTH'] = 1
                df.loc[(df['SCORE_LENGTH'] > 0.5) & (df['SCORE_LENGTH'] <= .75), 'RANKING_LENGTH'] = 2
                df.loc[(df['SCORE_LENGTH'] > 0.75) & (df['SCORE_LENGTH'] <= 1.25), 'RANKING_LENGTH'] = 3
                df.loc[(df['SCORE_LENGTH'] > 1.25) & (df['SCORE_LENGTH'] <= 1.5), 'RANKING_LENGTH'] = 4
                df.loc[df['SCORE_LENGTH' ] > 1.5, 'RANKING_LENGTH'] = 5
                df.loc[(df['SCORE_LENGTH'] == -1) & (df['NEW_LENGTH'] > 0), 'RANKING_LENGTH'] = 5
                df.loc[(df['SCORE_LENGTH'] == -1) & (df['NEW_LENGTH'] <= 0), 'RANKING_LENGTH'] = 1
                df.loc[(df['DIFF_LENGTH'] < 0), 'RANKING_LENGTH'] = -1 * df['RANKING_LENGTH']
            elif m == "POLYGON":
                df['RANKING_AREA'] = 0.0
                df['SCORE_AREA'] = 0.0
                df['DIFF_AREA'] = df['NEW_AREA'] - df['OLD_AREA']
                df.loc[df['OLD_AREA'] > 0, 'SCORE_AREA'] = df['NEW_AREA']/df['OLD_AREA']
                df.loc[df['OLD_AREA'] == 0, 'SCORE_AREA'] = -1
                df.loc[(df['SCORE_AREA'] >= 0) & (df['SCORE_AREA'] <= .5), 'RANKING_AREA'] = 1
                df.loc[(df['SCORE_AREA'] > 0.5) & (df['SCORE_AREA'] <= .75), 'RANKING_AREA'] = 2
                df.loc[(df['SCORE_AREA'] > 0.75) & (df['SCORE_AREA'] <= 1.25), 'RANKING_AREA'] = 3
                df.loc[(df['SCORE_AREA'] > 1.25) & (df['SCORE_AREA'] <= 1.5), 'RANKING_AREA'] = 4
                df.loc[df['SCORE_AREA' ] > 1.5, 'RANKING_AREA'] = 5
                df.loc[(df['SCORE_AREA'] == -1) & (df['NEW_AREA'] > 0), 'RANKING_AREA'] = 5
                df.loc[(df['SCORE_AREA'] == -1) & (df['NEW_AREA'] <= 0), 'RANKING_AREA'] = 1
                df.loc[(df['DIFF_AREA'] < 0), 'RANKING_AREA'] = -1 * df['RANKING_AREA']
            del m
        df.to_csv(tcsv, columns=df.columns.tolist(), index=False)
        return arcpy.CopyRows_management(tcsv, tcsv_fgdb)[0], df.columns.tolist()

    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "calculate_frequency_ranking",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
#--------------------------------------------------------------------------
def data_comparison(in_grid,
                    in_fcs,
                    in_old_gdb,
                    in_new_gdb,
                    out_grid,
                    geom_type="POINT",
                    scratchGDB=env.scratchGDB):
    """
    Generates rankings based on a given grid and data type

    Inputs:
     in_grid: path to area of interests
     in_fcs: list of feature class names
     in_old_gdb: old FGDB path for comparison
     in_new_gdb: new FGDB path for comparison
     out_grid: path of the output GDB feature class
     geom_type: string value of POINT, POLYLINE, or POLYGON
    """
    try:
        merged_points_n = os.path.join("in_memory", "merged_pts_n")#scratchGDB
        merged_points_o = os.path.join("in_memory", "merged_pts_o")#scratchGDB
        temp_out_grid = os.path.join(scratchGDB, "grid")
        old_stats = os.path.join(scratchGDB, "old_stats")
        new_stats = os.path.join(scratchGDB, "new_stats")
        # Copy Grid to Temp Folder
        temp_out_grid = arcpy.CopyFeatures_management(in_grid, temp_out_grid)[0]
        # Merge all fcs into one fc
        merged_points_n, total_n_count = merge_fcs(in_fcs,
                                                   merged_points_n,
                                                   gdb=in_new_gdb)
        merged_points_o, total_o_count = merge_fcs(in_fcs,
                                                   merged_points_o,
                                                   gdb=in_old_gdb)
        # intersect the grid
        new_pts_int = arcpy.Intersect_analysis(in_features=[merged_points_n, temp_out_grid],
                                               out_feature_class=os.path.join(scratchGDB, "new_pts_int"),
                                               join_attributes="ONLY_FID")[0]
        old_pts_int = arcpy.Intersect_analysis(in_features=[merged_points_o, temp_out_grid],
                                                out_feature_class=os.path.join(scratchGDB, "old_pts_int"),
                                                join_attributes="ONLY_FID")[0]
        if geom_type.lower() == "point":
            stat_fields_new = "FID_grid COUNT"
            stat_fields_old = "FID_grid COUNT"
            method = ["POINT"]
        elif geom_type.lower() in ("polyline", 'polygon'):
            arcpy.AddField_management(old_pts_int, "OLD_LENGTH", "FLOAT")
            arcpy.CalculateField_management(in_table=old_pts_int,
                                            field="OLD_LENGTH", expression="!shape.length@kilometers!",
                                            expression_type="PYTHON_9.3", code_block="")
            arcpy.AddField_management(new_pts_int, "NEW_LENGTH", "FLOAT")
            arcpy.CalculateField_management(in_table=new_pts_int,
                                            field="NEW_LENGTH", expression="!shape.length@kilometers!",
                                            expression_type="PYTHON_9.3", code_block="")
            if geom_type.lower() == "polygon":
                arcpy.AddField_management(old_pts_int, "OLD_AREA", "FLOAT")
                #!shape.area@squarekilometers!
                arcpy.CalculateField_management(in_table=old_pts_int,
                                                field="OLD_AREA", expression="!shape.area@squarekilometers!",
                                                expression_type="PYTHON_9.3", code_block="")
                arcpy.AddField_management(new_pts_int, "NEW_AREA", "FLOAT")
                arcpy.CalculateField_management(in_table=new_pts_int,
                                                field="NEW_AREA", expression="!shape.area@squarekilometers!",
                                                expression_type="PYTHON_9.3", code_block="")
            if geom_type.lower() == "polygon":
                stat_fields_new = "FID_grid COUNT;NEW_LENGTH SUM;NEW_AREA SUM"
                stat_fields_old = "FID_grid COUNT;OLD_LENGTH SUM;OLD_AREA SUM"
                method = ['POINT', 'POLYLINE', 'POLYGON']
            else:
                stat_fields_new = "FID_grid COUNT;NEW_LENGTH SUM"
                stat_fields_old = "FID_grid COUNT;OLD_LENGTH SUM"
                method = ['POINT', 'POLYLINE']

        # get the counts
        old_stats = arcpy.Statistics_analysis(in_table=old_pts_int,
                                              out_table=old_stats,
                                              statistics_fields=stat_fields_old,
                                              case_field="FID_grid")[0]
        new_stats = arcpy.Statistics_analysis(in_table=new_pts_int,
                                              out_table=new_stats,
                                              statistics_fields=stat_fields_new,
                                              case_field="FID_grid")[0]
        # join the old stats to the new stats
        if geom_type.lower() == "polygon":
            arcpy.AlterField_management(new_stats, field="SUM_NEW_LENGTH",
                                        new_field_name="NEW_LENGTH")
            arcpy.AlterField_management(new_stats, field="SUM_NEW_AREA",
                                        new_field_name="NEW_AREA")
            out_fields = ['FID_grid', 'FREQUENCY', 'SUM_OLD_LENGTH', 'SUM_OLD_AREA']
            ndt = np.dtype([('FID_grid', '<i4'), ('OLD_FREQUENCY', '<i4'),
                            ('OLD_LENGTH', np.float64), ('OLD_AREA', np.float64)])
            export_fields = ['FID_grid',
                             'FREQUENCY','OLD_FREQUENCY',
                             'OLD_LENGTH', 'NEW_LENGTH',
                             'OLD_AREA', 'NEW_AREA',
                             'SCORE','RANKING']
        elif geom_type.lower() == "point":
            out_fields = ['FID_grid', 'FREQUENCY']
            ndt = np.dtype([('FID_grid', '<i4'), ('OLD_FREQUENCY', '<i4')])
            export_fields = ['FID_grid', 'FREQUENCY','OLD_FREQUENCY', 'SCORE','RANKING']
        elif geom_type.lower() == "polyline":
            arcpy.AlterField_management(new_stats, field="SUM_NEW_LENGTH",
                                        new_field_name="NEW_LENGTH")
            out_fields = ['FID_grid', 'FREQUENCY', 'SUM_OLD_LENGTH']
            ndt = np.dtype([('FID_grid', '<i4'), ('OLD_FREQUENCY', '<i4'),
                            ('OLD_LENGTH', np.float64)])
            export_fields = ['FID_grid',
                             'FREQUENCY','OLD_FREQUENCY',
                             'OLD_LENGTH', 'NEW_LENGTH',
                             'SCORE','RANKING']
        old_array = da.TableToNumPyArray(in_table=old_stats,
                                         field_names=out_fields)
        old_array.dtype = ndt
        # Add SCORE and RANKING fields and remove unneeded fields
        da.ExtendTable(new_stats, "FID_grid",
                       old_array, "FID_grid", False)
        array = np.array([],
                         np.dtype([('_id',np.int32),
                                   ('SCORE', np.float64),
                                   ('RANKING', np.int64)]))
        da.ExtendTable(new_stats,
                       arcpy.Describe(new_stats).OIDFieldName,
                       array,
                       "_id", False)
        arcpy.DeleteField_management(new_stats, ['COUNT_FID_grid'])
        array = da.TableToNumPyArray(in_table=new_stats,
                                    field_names=export_fields,
                                    null_value=0)
        # Calculate the rankings
        tcsv, column_list = calculate_frequency_ranking(array=array,
                                                        methods=method)
        array = da.TableToNumPyArray(tcsv, column_list)
        da.ExtendTable(temp_out_grid,
                       arcpy.Describe(temp_out_grid).OIDFieldName,
                       array,
                       "FID_grid")
        # Clean up NULL values
        if geom_type.lower() == "point":
            sql = """RANKING IS NULL"""
            fields = ['RANKING']
        elif geom_type.lower() == "polyline":
            sql = """RANKING IS NULL OR RANKING_LENGTH IS NULL"""
            fields = ['RANKING', 'RANKING_LENGTH']
        elif geom_type.lower() == "polygon":
            sql = """RANKING IS NULL OR RANKING_LENGTH IS NULL OR RANKING_AREA IS NULL"""
            fields = ['RANKING', 'RANKING_LENGTH', 'RANKING_AREA']
        with da.UpdateCursor(temp_out_grid, fields,
                             where_clause=sql) as urows:
            for row in urows:
                if geom_type.lower() == "point":
                    row[0] = 0
                elif geom_type.lower() == "polyline":
                    if row[0] is None:
                        row[0] = 0
                    if row[1] is None:
                        row[1] = 0
                elif geom_type.lower() == "polygon":
                    if row[0] is None:
                        row[0] = 0
                    if row[1] is None:
                        row[1] = 0
                    if row[2] is None:
                        row[2] = 0
                urows.updateRow(row)
                del row
        del urows
        # return the output grid
        return arcpy.CopyFeatures_management(temp_out_grid, out_grid)[0]
    except:
        line, filename, synerror = trace()
        raise FunctionError(
                {
                "function": "data_comparison",
                "line": line,
                "filename": filename,
                "synerror": synerror,
                "arc" : str(arcpy.GetMessages(2))
                }
                )
#--------------------------------------------------------------------------
def gather_fcs(workspace, check=False, other=None):
    """collects the points, lines and polygons within
    a given workspace"""
    wrk_old = env.workspace
    if check:
        env.workspace = other
        check_fcs = arcpy.ListFeatureClasses()
    env.workspace = workspace
    ds = {
        "POINTS" : {},
        "POLYLINES" : {},
        "POLYGONS": {}
    }
    if check:
        pts = [pt for pt in arcpy.ListFeatureClasses(feature_type="Point") if pt in check_fcs]
        lines = [pt for pt in arcpy.ListFeatureClasses(feature_type="Polyline") if pt in check_fcs]
        polygons = [pt for pt in arcpy.ListFeatureClasses(feature_type="Polygon") if pt in check_fcs]
    else:
        pts = [pt for pt in arcpy.ListFeatureClasses(feature_type="Point")]
        lines = [pt for pt in arcpy.ListFeatureClasses(feature_type="Polyline")]
        polygons = [pt for pt in arcpy.ListFeatureClasses(feature_type="Polygon")]
    ds['POINTS'] = pts
    ds['POLYLINES'] = lines
    ds['POLYGONS'] = polygons
    env.workspace = wrk_old
    return ds
#--------------------------------------------------------------------------
def merge_fcs(fcs, merged_fc, gdb):
    """combines like geometries into a feature class"""

    desc = arcpy.Describe(os.path.join(gdb, fcs[0]))
    if arcpy.Exists(merged_fc):
        arcpy.Delete_management(merged_fc)
    ifc = arcpy.CreateFeatureclass_management(out_path=os.path.dirname(merged_fc),
                                              out_name=os.path.basename(merged_fc),
                                              geometry_type=desc.shapeType.upper(),
                                              spatial_reference=desc.spatialReference)[0]
    icur = da.InsertCursor(ifc, ['SHAPE@'])
    count = 0
    for fc in fcs:
        fc = os.path.join(gdb, fc)
        with da.SearchCursor(fc, ["SHAPE@"]) as rows:
            for row in rows:
                icur.insertRow(row)
                count += 1
                del row
        del rows
        del fc
    del icur, desc
    return ifc, count
#--------------------------------------------------------------------------
def main(*argv):
    """ main driver of program """
    try:
        old_gdb = argv[0]#
        new_gdb = argv[1]#
        grid_fc = argv[2]#
        out_gdb = argv[3]#
        #  Local Variable
        #
        scratchGDB = env.scratchGDB
        output_fc_pts = os.path.join(out_gdb,"grid_pts")
        output_fc_lns = os.path.join(out_gdb,"grid_lns")
        output_fc_ply = os.path.join(out_gdb,"grid_ply")
        results = []
        #  Logic
        #
        mt_now = datetime.datetime.now()
        if arcpy.Exists(out_gdb) == False:
            arcpy.CreateFileGDB_management(out_folder_path=os.path.dirname(out_gdb),
                                          out_name=os.path.basename(out_gdb))
        compare_fcs = gather_fcs(workspace=new_gdb, check=True, other=old_gdb)

        for feature_type in compare_fcs.keys():
            if feature_type == "POINTS":

                arcpy.AddMessage("... Processing Points ...")
                output_fc_pts = data_comparison(in_grid=grid_fc,
                                                in_fcs=compare_fcs[feature_type],
                                                in_old_gdb=old_gdb,
                                                in_new_gdb=new_gdb,
                                                out_grid=output_fc_pts,
                                                geom_type="POINT")
                results.append(output_fc_pts)
            elif feature_type == "POLYLINES":
                arcpy.AddMessage("... Processing Polylines ...")
                output_fc_lns = data_comparison(in_grid=grid_fc,
                                                in_fcs=compare_fcs[feature_type],
                                                in_old_gdb=old_gdb,
                                                in_new_gdb=new_gdb,
                                                out_grid=output_fc_lns,
                                                geom_type="POLYLINE")
                results.append(output_fc_lns)
            elif feature_type == "POLYGONS":
                arcpy.AddMessage("... Processing Polygons ...")
                output_fc_ply = data_comparison(in_grid=grid_fc,
                                                in_fcs=compare_fcs[feature_type],
                                                in_old_gdb=old_gdb,
                                                in_new_gdb=new_gdb,
                                                out_grid=output_fc_ply,
                                                geom_type="POLYGON")
                results.append(output_fc_ply)
        arcpy.AddMessage("... %s %s ..." % ("TOTAL PROCESSING TIME: ", datetime.datetime.now() - mt_now))
        arcpy.SetParameterAsText(4, output_fc_pts)
        arcpy.SetParameterAsText(5, output_fc_lns)
        arcpy.SetParameterAsText(6, output_fc_ply)
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
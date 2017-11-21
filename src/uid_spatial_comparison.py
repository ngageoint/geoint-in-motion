"""-----------------------------------------------------------------------------
Name: uid_spatial_comparison.py
Purpose: Finds features that have been added, removed, or had their geometry
        modified between two instances of time.
Description: Takes two features classes as inputs and determines what features
        have been added, removed, or had their geometries modified. The input
        feature classes need to have a unique ID field in order to determine
        the additions, removals, and deletions.
Requirements: Python 2.7.x/Python3.x, ArcGIS 10.4+/Pro 1.2+
Author(s): Andrew Chapkowski, Contractor for National Geospatial-Intelligence
        Agency (NGA) | Gregory Brunner, Contractor NGA
Program Manager: Derek Silva, NGA (Derek.A.Silva@nga.mil)
Created: May, 2017
Modified:
Copyright: Esri
License:
-----------------------------------------------------------------------------"""
import pandas as pd
import numpy as np
import arcgis
import arcpy
import sys
import os

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
def main(*argv):
    """ main driver of program """
    try:
        if os.path.split(sys.executable)[1] == 'ArcGISPro.exe':

            # Expected Parameters
            in_new = argv[0]
            in_old = argv[1]
            unique = argv[2]
            out_db = argv[3]

            scratch_gdb = arcpy.env.scratchGDB
            dis_new_path = os.path.join(scratch_gdb, "dis_new")
            dis_old_path = os.path.join(scratch_gdb, "dis_old")

            dis_new = arcpy.Dissolve_management(
                in_features=in_new,
                out_feature_class=dis_new_path,
                dissolve_field=unique
            )[0]

            dis_old = arcpy.Dissolve_management(
                in_features=in_old,
                out_feature_class=dis_old_path,
                dissolve_field=unique
            )[0]

            new_sdf = arcgis.features.SpatialDataFrame.from_featureclass(dis_new)
            old_sdf = arcgis.features.SpatialDataFrame.from_featureclass(dis_old)

        else:

            # Expected Parameters
            in_new = argv[0]
            in_old = argv[1]
            unique = argv[2]
            out_db = argv[3]
            type = argv[4]

            if type.lower() not in ['fc', 'fs', 'sdf']:
                raise Exception('Input Type Not In Accepted Options: fc | fs | sdf')

            else:
                if type.lower() == 'fc':

                    scratch_gdb = arcpy.env.scratchGDB
                    dis_new_path = os.path.join(scratch_gdb, "dis_new")
                    dis_old_path = os.path.join(scratch_gdb, "dis_old")

                    dis_new = arcpy.Dissolve_management(
                        in_features=in_new,
                        out_feature_class=dis_new_path,
                        dissolve_field=unique
                    )[0]

                    dis_old = arcpy.Dissolve_management(
                        in_features=in_old,
                        out_feature_class=dis_old_path,
                        dissolve_field=unique
                    )[0]

                    new_sdf = arcgis.features.SpatialDataFrame.from_featureclass(dis_new)
                    old_sdf = arcgis.features.SpatialDataFrame.from_featureclass(dis_old)

                elif type.lower() == 'fs':

                    gis = arcgis.gis.GIS("pro")

                    new_fl = arcgis.features.FeatureLayer(in_old, gis=gis)
                    old_fl = arcgis.features.FeatureLayer(in_old, gis=gis)

                    new_sdf = arcgis.features.SpatialDataFrame.from_layer(new_fl)
                    newcols = new_sdf.columns.get_values().tolist()
                    new_sdf.drop([col for col in newcols if col not in [unique, 'SHAPE']], axis=1, inplace=True)

                    old_sdf = arcgis.features.SpatialDataFrame.from_layer(old_fl)
                    oldcols = old_sdf.columns.get_values().tolist()
                    old_sdf.drop([col for col in oldcols if col not in [unique, 'SHAPE']], axis=1, inplace=True)

                else:

                    newcols = in_new.columns.get_values().tolist()
                    in_new.drop([col for col in newcols if col not in [unique, 'SHAPE']], axis=1, inplace=True)
                    new_sdf = in_new

                    oldcols = in_old.columns.get_values().tolist()
                    in_old.drop([col for col in oldcols if col not in [unique, 'SHAPE']], axis=1, inplace=True)
                    old_sdf = in_old

        # Find Added and Removed Features
        unew = set(new_sdf[unique].unique().tolist())
        uold = set(old_sdf[unique].unique().tolist())

        adds = list(unew - uold)
        dels = list(uold - unew)

        old_df = old_sdf[old_sdf[unique].isin(dels)].copy()
        old_df['STATUS'] = "REMOVED FEATURE"

        new_df = new_sdf[new_sdf[unique].isin(adds)].copy()
        new_df['STATUS'] = "NEW FEATURE"

        # Find Geometry Differences
        df2 = new_sdf[~new_sdf[unique].isin(adds)].copy()
        df2.index = df2[unique]

        df1 = old_sdf[~old_sdf[unique].isin(dels)].copy()
        df1.index = df1[unique]

        # Merge DataFrames & Assert Geometry Equality
        merged = pd.merge(df2, df1, on=[unique])
        merged.index = merged[unique]
        merged['STATUS'] = np.where(
            merged['SHAPE_x'].equals(merged['SHAPE_y']),
            'GEOMETRY CONSISTENT', 'GEOMETRY MODIFIED'
        )

        # Drop MISC Fields Created During Join - Keep SHAPE For DF2/SHAPE_X
        merged['SHAPE'] = merged['SHAPE_x']
        merged.drop('SHAPE_x', axis=1, inplace=True)
        merged.drop('SHAPE_y', axis=1, inplace=True)
        merged.drop('OBJECTID_x', axis=1, inplace=True)
        merged.drop('OBJECTID_y', axis=1, inplace=True)
        merged.reset_index(inplace=True, drop=True)

        # Join 3 Analysis DataFrames & Export to Feature Class
        joined = pd.concat([merged, old_df, new_df])
        joined.reset_index(inplace=True, drop=True)
        out_fc = joined.to_featureclass(out_db, "modifed_dataset_check")

        # Cleanup
        del new_sdf
        del old_sdf
        del new_df
        del old_df
        del merged
        del joined
        del df1
        del df2

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
    arcpy.env.overwriteOutput = True
    argv = tuple(arcpy.GetParameterAsText(i)
    for i in range(arcpy.GetArgumentCount()))
    main(*argv)
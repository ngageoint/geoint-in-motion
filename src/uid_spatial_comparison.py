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
import os
import sys
import pandas as pd
import arcpy
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping
from geodataset import SpatialDataFrame
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
        fc_new = argv[0]
        fc_old = argv[1]
        unique_field = argv[2]
        out_gdb = argv[3]

        #  Local Variables
        #
        scratchGDB = env.scratchGDB
        dis_new = os.path.join(scratchGDB, "dis_new")
        dis_old = os.path.join(scratchGDB, "dis_old")
        out_fc = os.path.join(out_gdb, os.path.basename(fc_old))
        #  Logic
        #
        dis_new = arcpy.Dissolve_management(in_features=fc_new,
                                            out_feature_class=dis_new,
                                            dissolve_field=unique_field)[0]
        dis_old = arcpy.Dissolve_management(in_features=fc_old,
                                            out_feature_class=dis_old,
                                            dissolve_field=unique_field)[0]
        fields = [unique_field, 'SHAPE@']
        new_sdf = SpatialDataFrame.from_featureclass(dis_new, fields=[unique_field])
        old_sdf = SpatialDataFrame.from_featureclass(dis_old, fields=[unique_field])
        #  Find Added and Removed Features
        #
        unew = set(new_sdf[unique_field].unique().tolist())
        uold = set(old_sdf[unique_field].unique().tolist())
        adds = list(unew - uold)
        deletes = list(uold - unew)
        old_df = old_sdf[old_sdf[unique_field].isin(deletes)].copy()
        old_df['STATUS'] = "REMOVED FEATURE"
        new_df = new_sdf[new_sdf[unique_field].isin(adds)].copy()
        new_df['STATUS'] = "NEW FEATURE"
        # Find Geometry Differences
        #
        df2 = new_sdf[~new_sdf[unique_field].isin(adds)].copy()
        df2.index = df2[unique_field]
        df1 = old_sdf[~old_sdf[unique_field].isin(deletes)].copy()
        df1.index = df1[unique_field]
        ne = df1 != df2
        ne = ne['SHAPE']
        updated = df2[ne].copy()
        updated['STATUS'] = "GEOMETRY MODIFIED"
        updated.reset_index(inplace=True, drop=True)
        del ne
        del df1
        del df2
        del new_sdf
        del old_sdf
        joined = pd.concat([updated,
                            old_df,
                            new_df])
        joined.reset_index(inplace=True, drop=True)
        del updated
        del new_df
        del old_df
        out_fc = joined.to_featureclass(out_gdb, "modifed_dataset")
        del joined
        arcpy.SetParameterAsText(4, out_fc)
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
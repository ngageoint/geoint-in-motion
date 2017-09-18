"""-----------------------------------------------------------------------------
Name: uid_attribute_checking.py
Purpose: Find features that have had their attributes changed.
Description: Takes two features classes as inputs and determines what features
        have had attributes modified in any way. The input feature classes need:
            1. A unique ID field in order to determine feature attribute
                changes.
            2. To be in the same schema (attribute fields must be identical).
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

import arcpy
from arcpy import env
from arcpy import da
if sys.version_info.major == 3:
    from arcpy import mp as mapping
else:
    from arcpy import mapping
import numpy as np
import pandas as pd
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
        new_fc = argv[0]
        uid_field = argv[1]
        old_fc = argv[2]
        out_gdb = argv[3]
        #  Local Variables
        #
        scratchGDB = env.scratchGDB
        scratchFolder = env.scratchFolder
        out_table = os.path.join(out_gdb, "InformationTable")
        out_fc = os.path.join(out_gdb, "changed_features")
        change_csv = os.path.join(env.scratchFolder, "changes.csv")
        change_table = os.path.join(out_gdb, "change_table")
        adds_fc = None
        removes_fc = None
        #  Logic
        #
        if arcpy.Exists(out_table):
            arcpy.Delete_management(out_table)
        if arcpy.Exists(out_fc):
            arcpy.Delete_management(out_fc)
        tbl = arcpy.CreateTable_management(out_path=out_gdb, out_name="InformationTable")[0]
        array = np.array([],
                         np.dtype([('_id',np.int32),
                                   ('OLD_COUNT', '|S25'),
                                   ('NEW_COUNT', '|S25'),
                                   ('REMOVE_FIELDS', '|S256'),
                                   ('ADDED_FIELDS', '|S256'),
                                   ('SR_OLD', '|S256'),
                                   ('SR_NEW', '|S256')])
                         )

        arcpy.da.ExtendTable(tbl,
                                 arcpy.Describe(tbl).OIDFieldName,
                                 array,
                                 "_id")
        del array
        fields_old = set([field.name for field in arcpy.ListFields(old_fc)])
        fields_new = set([field.name for field in arcpy.ListFields(new_fc)])
        desc_old = arcpy.Describe(old_fc)
        desc_new = arcpy.Describe(new_fc)
        row = [str(arcpy.GetCount_management(old_fc)[0]),
               str(arcpy.GetCount_management(new_fc)[0]),
               ",".join(list(fields_old - fields_new)),
               ",".join(list(fields_new - fields_old)),
               str(desc_old.spatialReference.factoryCode),
               str(desc_new.spatialReference.factoryCode)
               ]
        #del desc_new, desc_old
        icur = arcpy.da.InsertCursor(tbl, ['OLD_COUNT', 'NEW_COUNT', 'REMOVE_FIELDS',
                                           'ADDED_FIELDS', 'SR_OLD', 'SR_NEW'])
        icur.insertRow(row)
        del icur, row
        # 2). Output Feature Class: Geometry, ACTION, Summary of differences
        old_sdf = SpatialDataFrame.from_featureclass(old_fc)
        len_old_sfd = len(old_sdf)
        old_sdf = old_sdf.drop_duplicates(subset = uid_field, keep = False)
        len_old_sfd_after = len(old_sdf)
        if len_old_sfd-len_old_sfd_after != 0:
            arcpy.AddMessage(len_old_sfd)
            arcpy.AddMessage(len_old_sfd_after)
            arcpy.AddMessage("Dropping Dublicates from Old Feature Classs")

        if arcpy.Describe(old_fc).oidFieldName in old_sdf.columns:
            arcpy.AddMessage("deleting oid field")
            del old_sdf[arcpy.Describe(old_fc).oidFieldName]
        new_sdf = SpatialDataFrame.from_featureclass(new_fc)
        len_new_sfd = len(new_sdf)
        new_sdf = new_sdf.drop_duplicates(subset = uid_field, keep = False)
        len_new_sfd_after = len(new_sdf)
        if len_new_sfd-len_new_sfd_after != 0:
            arcpy.AddMessage(len_new_sfd)
            arcpy.AddMessage(len_new_sfd_after)
            arcpy.AddMessage("Dropping Dublicates from New Feature Class")

        if arcpy.Describe(new_fc).oidFieldName in new_sdf.columns:
            arcpy.AddMessage("deleting oid field")
            del new_sdf[arcpy.Describe(new_fc).oidFieldName]

        # A). Find Adds, Deletes and Matching Values
        old_uids = set(old_sdf[uid_field].unique().tolist())
        new_uids = set(new_sdf[uid_field].unique().tolist())
        common_uids = list(new_uids.intersection(old_uids))
        fields = [field for field in old_sdf.columns.tolist() \
                      if field in new_sdf.columns.tolist()]
        if pd.__version__ == "0.16.1":
            old_sdf['temp_key'] = old_sdf[uid_field]
            new_sdf['temp_key'] = new_sdf[uid_field]
            merged = old_sdf.merge(new_sdf, on=['temp_key'], how='outer', suffixes=['', '_'])
            adds = merged.loc[merged[uid_field + "_"].notnull() & merged[uid_field].isnull()]
            deletes = merged.loc[merged[uid_field + "_"].isnull() & merged[uid_field].notnull()]
            ignore_ids = adds[uid_field+'_'].unique().tolist() + deletes[uid_field].unique().tolist()
        else:#if pd.__version__ == "foo":
            merged = old_sdf.merge(new_sdf, on=[uid_field], how='outer',
                                   suffixes=['', '_'], indicator=True)
            adds = merged.loc[merged._merge.eq('right_only')]
            deletes = merged.loc[merged._merge.eq('left_only')]
            ignore_ids = adds[uid_field].unique().tolist() + deletes[uid_field].unique().tolist()
        if len(adds) > 0:
            q = new_sdf[uid_field].isin(adds[uid_field].tolist())
            adds_fc = new_sdf[q].to_featureclass(out_location=out_gdb,
                                              out_name="added_features",
                                              overwrite=True,
                                              skip_invalid=True)
        if len(deletes) > 0:
            q = old_sdf[uid_field].isin(deletes[uid_field].tolist())
            removes_fc = old_sdf[q].to_featureclass(out_location=out_gdb,
                                                    out_name="deleted_features",
                                                    overwrite=True,
                                                    skip_invalid=True)
        # Compare Attributes of SDF
        fields = [field for field in old_sdf.columns.tolist() \
                  if field in new_sdf.columns.tolist()]
        if 'SHAPE' in fields:
            fields.remove("SHAPE")
            #del old_sdf["SHAPE"]
            #del new_sdf["SHAPE"]

        cq = new_sdf[uid_field].isin(common_uids)
        cq1 = old_sdf[uid_field].isin(common_uids)
        old_sdf = old_sdf[cq1].copy()
        new_sdf = new_sdf[cq].copy()

        new_sdf.index = new_sdf[uid_field]
        old_sdf.index = old_sdf[uid_field]
        old_sdf.dtypes
        new_sdf.dtypes
        old_sdf.sort_index(inplace=True)
        print(old_sdf.head())
        new_sdf.sort_index(inplace=True)
        print(new_sdf.head())
        ne = (old_sdf[fields] != new_sdf[fields]).any(1)
        ne_stacked = (old_sdf[fields] != new_sdf[fields]).stack()
        changed = ne_stacked[ne_stacked]

        changed.index.names = [uid_field, 'col']
        difference_locations = np.where(old_sdf[fields] != new_sdf[fields])
        changed_from = new_sdf[fields].values[difference_locations]
        changed_to = old_sdf[fields].values[difference_locations]
        df_new = pd.DataFrame({'from_val': changed_from, 'to_val': changed_to}, index=changed.index)
        df_new.reset_index(level=['col'],inplace=True)
        q3 = df_new['from_val'].isnull() & df_new['to_val'].isnull()
        df_new[~q3].to_csv(change_csv)
        joined_sdf = SpatialDataFrame.merge(new_sdf, df_new[~q3], right_index=True, left_index=True)
    ##    arcpy.AddMessage('everywhere')
    ##    arcpy.AddMessage(old_sdf)
    ##    arcpy.AddMessage('----------------')
    ##    arcpy.AddMessage(joined_sdf)
    ##    arcpy.AddMessage('----------------')
        q4 = joined_sdf['from_val'].isnull() & joined_sdf['to_val'].isnull()
        stripped_sdf = joined_sdf[~q4]
        stripped_sdf.drop('from_val', axis=1, inplace=True)
        stripped_sdf.drop('to_val', axis=1, inplace=True)
        stripped_sdf.drop('col', axis=1, inplace=True)
        stripped_sdf['Edit Count'] = stripped_sdf.groupby([uid_field]).size()
        stripped_sdf.drop_duplicates(subset = uid_field, keep = 'last', inplace=True)
        #arcpy.AddMessage(stripped_sdf)
        #arcpy.AddMessage('----------------')
        #joined_sdf.sr = desc_new.spatialReference
        stripped_sdf.to_featureclass(out_location=out_gdb,
                                                    out_name="changed_features",
                                                    overwrite=True,
                                                    skip_invalid=True)
        arcpy.AddMessage('Done.')
        change_tbl = arcpy.CopyRows_management(change_csv, change_table)[0]
        arcpy.SetParameterAsText(4, change_tbl) # Change Table
        arcpy.SetParameterAsText(5, tbl) # Information Table
        arcpy.SetParameterAsText(6, out_fc)
        if adds_fc:
            arcpy.SetParameterAsText(7, adds_fc) # added rows
        if removes_fc:
            arcpy.SetParameterAsText(8, removes_fc) # deleted rows
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
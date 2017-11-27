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
def build_information_table(db, new, old):
    # Set Overview Differences
    tbl = arcpy.CreateTable_management(out_path=db, out_name="InformationTable")[0]
    array = np.array([],
                     np.dtype([('_id', np.int32),
                               ('OLD_COUNT', '|S25'),
                               ('NEW_COUNT', '|S25'),
                               ('REMOVE_FIELDS', '|S256'),
                               ('ADDED_FIELDS', '|S256'),
                               ('SR_OLD', '|S256'),
                               ('SR_NEW', '|S256')])
                     )

    arcpy.da.ExtendTable(tbl, arcpy.Describe(tbl).OIDFieldName, array, "_id")
    del array

    desc_old = arcpy.Describe(old)
    desc_new = arcpy.Describe(new)
    fields_old = set([field.name for field in arcpy.ListFields(old)])
    fields_new = set([field.name for field in arcpy.ListFields(new)])

    # Handle Field Concatenation (Except Used for Edge Cases)
    try:
        row = [str(arcpy.GetCount_management(old)[0]),
               str(arcpy.GetCount_management(new)[0]),
               ",".join(list(fields_old - fields_new)),
               ",".join(list(fields_new - fields_old)),
               str(desc_old.spatialReference.factoryCode),
               str(desc_new.spatialReference.factoryCode)
               ]

        cursor = arcpy.da.InsertCursor(
            tbl,
            ['OLD_COUNT', 'NEW_COUNT', 'REMOVE_FIELDS', 'ADDED_FIELDS', 'SR_OLD', 'SR_NEW']
        )
        cursor.insertRow(row)
        del cursor, row

    except:
        row = [str(arcpy.GetCount_management(old)[0]),
               str(arcpy.GetCount_management(new)[0]),
               ' ',
               ' ',
               str(desc_old.spatialReference.factoryCode),
               str(desc_new.spatialReference.factoryCode)
               ]

        cursor = arcpy.da.InsertCursor(
            tbl,
            ['OLD_COUNT', 'NEW_COUNT', 'REMOVE_FIELDS', 'ADDED_FIELDS', 'SR_OLD', 'SR_NEW']
        )
        cursor.insertRow(row)
        del cursor, row
#--------------------------------------------------------------------------
def handle_duplicates(sdf_set, unique):

    len_sfd = len(sdf_set[0])
    sdf = sdf_set[0].drop_duplicates(subset=unique, keep=False)
    len_sfd_after = len(sdf)

    if len_sfd - len_sfd_after != 0:
        arcpy.AddMessage(len_sfd)
        arcpy.AddMessage(len_sfd_after)
        arcpy.AddMessage("Dropping Dublicates from Old Feature Classs")

    if arcpy.Describe(sdf_set[1]).oidFieldName in sdf.columns:
        arcpy.AddMessage("deleting oid field")
        sdf_set[0].drop(arcpy.Describe(sdf_set[1]).oidFieldName, axis=1, inplace=True)
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

        else:

            # Expected Parameters
            in_new = argv[0]
            in_old = argv[1]
            unique = argv[2]
            out_db = argv[3]
            t_flag = argv[4]

            if t_flag.lower() not in ['fc', 'fs', 'sdf']:
                raise Exception('Input Type Not In Accepted Options: fc | fs | sdf')

        #  Local Variables
        out_table    = os.path.join(out_db, "InformationTable")
        out_fc       = os.path.join(out_db, "changed_features")
        change_csv   = os.path.join(arcpy.env.scratchFolder, "changes.csv")
        change_table = os.path.join(out_db, "change_table")

        # Remove Existing Files
        for target in [out_fc, out_table]:
            if arcpy.Exists(target):
                arcpy.Delete_management(target)

        # Create Information Table (Overview of Differences)
        build_information_table(out_db, in_new, in_old)

        # Create SpatialDataFrame Objects
        old_sdf = arcgis.features.SpatialDataFrame.from_featureclass(in_old)
        new_sdf = arcgis.features.SpatialDataFrame.from_featureclass(in_new)

        # Remove Duplicate Values in old_sdf/new_sdf
        for sdf_set in [[old_sdf, in_old], [new_sdf, in_new]]:
            handle_duplicates(sdf_set, unique)

        # Find Adds, Deletes and Matching Values
        merged = pd.merge(old_sdf, new_sdf, on=[unique], how='outer', indicator=True)
        adds    = merged.loc[merged['_merge'] == 'right_only']
        deletes = merged.loc[merged['_merge'] == 'left_only']
        if len(adds) > 0:
            q = new_sdf[unique].isin(adds[unique].tolist())
            new_sdf[q].to_featureclass(
                out_location=out_db,
                out_name="added_features",
                overwrite=True,
                skip_invalid=True
            )
        if len(deletes) > 0:
            q = old_sdf[unique].isin(deletes[unique].tolist())
            old_sdf[q].to_featureclass(
                out_location=out_db,
                out_name="deleted_features",
                overwrite=True,
                skip_invalid=True
            )

        # Assess Changed Features
        fields = [field for field in old_sdf.columns.tolist() if field in new_sdf.columns.tolist()]
        if 'SHAPE' in fields:
            fields.remove("SHAPE")

        old_uids = set(old_sdf[unique].unique().tolist())
        new_uids = set(new_sdf[unique].unique().tolist())
        common_uids = list(new_uids.intersection(old_uids))
        cq = new_sdf[unique].isin(common_uids)
        cq1 = old_sdf[unique].isin(common_uids)
        old_sdf = old_sdf[cq1].copy()
        new_sdf = new_sdf[cq].copy()

        new_sdf.index = new_sdf[unique]
        old_sdf.index = old_sdf[unique]
        old_sdf.sort_index(inplace=True)
        new_sdf.sort_index(inplace=True)
        ne_stacked = (old_sdf[fields] != new_sdf[fields]).stack()
        changed = ne_stacked[ne_stacked]

        changed.index.names = [unique, 'col']
        difference_locations = np.where(old_sdf[fields] != new_sdf[fields])
        changed_from = new_sdf[fields].values[difference_locations]
        changed_to = old_sdf[fields].values[difference_locations]
        df_new = pd.DataFrame({'from_val': changed_from, 'to_val': changed_to}, index=changed.index)
        df_new.reset_index(level=['col'],inplace=True)
        q3 = df_new['from_val'].isnull() & df_new['to_val'].isnull()
        df_new[~q3].to_csv(change_csv)
        joined_sdf = arcgis.features.SpatialDataFrame.merge(new_sdf, df_new[~q3], right_index=True, left_index=True)

        q4 = joined_sdf['from_val'].isnull() & joined_sdf['to_val'].isnull()
        stripped_sdf = joined_sdf[~q4]
        stripped_sdf.drop('from_val', axis=1, inplace=True)
        stripped_sdf.drop('to_val', axis=1, inplace=True)
        stripped_sdf.drop('col', axis=1, inplace=True)
        stripped_sdf['Edit Count'] = stripped_sdf.groupby([unique]).size()
        stripped_sdf.drop_duplicates(subset=unique, keep='last', inplace=True)

        stripped_sdf.to_featureclass(
            out_location=out_db,
            out_name="changed_features",
            overwrite=True,
            skip_invalid=True
        )
        arcpy.CopyRows_management(change_csv, change_table)

        arcpy.AddMessage('Done.')

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
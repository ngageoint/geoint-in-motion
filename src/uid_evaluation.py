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
import tempfile
import arcgis
import arcpy
import time
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
def handle_duplicates(in_sdf, unique):
    len_sfd = len(in_sdf)
    sdf = in_sdf.drop_duplicates(subset=unique, keep=False)
    len_sfd_after = len(sdf)

    if len_sfd != len_sfd_after:
        print('Dropping Duplicate Rows Based on Unique Field: {}'.format(unique))
        in_sdf.drop_duplicates(subset=unique, keep=False, inplace=True)
#--------------------------------------------------------------------------
def att_main(old_sdf, new_sdf, unique, gis):

    # Remove Duplicate Row Based on Unique Field
    for sdf in [old_sdf, new_sdf]:
        handle_duplicates(sdf, unique)

    # Find Adds, Deletes and Matching Values
    merged  = pd.merge(old_sdf, new_sdf, on=[unique], how='outer', indicator=True)
    adds    = merged.loc[merged['_merge'] == 'right_only']
    deletes = merged.loc[merged['_merge'] == 'left_only']

    add_lyr = None
    if len(adds) > 0:
        print('Creating Additions Feature Layer')
        q = new_sdf[unique].isin(adds[unique].tolist())
        add_lyr = new_sdf[q].to_featurelayer(
            'Attribute_Additions_{}'.format(time.time()),
            gis=gis,
            tags='GEOINT'
        )

    del_lyr = None
    if len(deletes) > 0:
        print('Creating Deletions Feature Layer')
        q = old_sdf[unique].isin(deletes[unique].tolist())
        del_lyr = old_sdf[q].to_featurelayer(
            'Attribute_Deletions_{}'.format(time.time()),
            gis=gis,
            tags='GEOINT'
        )

    # Assess Changed Features
    fields = [
        field for field in old_sdf.columns.tolist()
        if field in new_sdf.columns.tolist()
        if field.lower() not in ['shape', 'objectid']
    ]

    old_uids    = set(old_sdf[unique].unique().tolist())
    new_uids    = set(new_sdf[unique].unique().tolist())
    common_uids = list(new_uids.intersection(old_uids))
    cq  = new_sdf[unique].isin(common_uids)
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

    # Add Change CSV to AGOL
    gis.content.add(
        {'title': 'ChangeCSV_{}'.format(time.time()), 'type': 'CSV', 'tags': 'GEOINT'},
        data=df_new[~q3].to_csv(tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=True))
    )

    joined_sdf = arcgis.features.SpatialDataFrame.merge(
        new_sdf,
        df_new[~q3],
        right_index=True,
        left_index=True
    )

    q4 = joined_sdf['from_val'].isnull() & joined_sdf['to_val'].isnull()
    stripped_sdf = joined_sdf[~q4]
    stripped_sdf.drop('from_val', axis=1, inplace=True)
    stripped_sdf.drop('to_val', axis=1, inplace=True)
    stripped_sdf.drop('col', axis=1, inplace=True)
    stripped_sdf['Edit Count'] = stripped_sdf.groupby([unique]).size()
    stripped_sdf.drop_duplicates(subset=unique, keep='last', inplace=True)

    print('Creating Attribute Change Feature Layer')
    chg_lyr = stripped_sdf.to_featurelayer(
        'Attribute_Changed_{}'.format(time.time()),
        gis=gis,
        tags='GEOINT'
    )

    # Return List of ArcGIS Online/Portal Items
    return [add_lyr, del_lyr, chg_lyr]
#--------------------------------------------------------------------------
def geo_main(old_sdf, new_sdf, unique, gis):

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
    print('Creating Spatial Change Feature Layer')
    spatial_lyr = joined.to_featurelayer(
        'Spatial_Updates_{}'.format(time.time()),
        gis=gis,
        tags='GEOINT'

    )

    # Cleanup
    del new_sdf
    del old_sdf
    del new_df
    del old_df
    del merged
    del joined
    del df1
    del df2

    return spatial_lyr
#--------------------------------------------------------------------------
def handle_service_conversion(url_list, gis):

    sdf_list = []
    for url in url_list:
        try:
            int(os.path.split(url)[0])
            feat_lyr = arcgis.features.FeatureLayer(url, gis=gis)
            sdf_list.append(arcgis.features.SpatialDataFrame.from_layer(feat_lyr))
        except ValueError:
            feat_lyr = arcgis.features.FeatureLayer('{}/0'.format(url), gis=gis)
            sdf_list.append(arcgis.features.SpatialDataFrame.from_layer(feat_lyr))

    return sdf_list
#--------------------------------------------------------------------------
def eval_service_attributes(old_url, new_url, unique, gis):

    old_sdf, new_sdf = handle_service_conversion([old_url, new_url], gis)

    return att_main(old_sdf, new_sdf, unique, gis)
#--------------------------------------------------------------------------
def eval_service_geometries(old_url, new_url, unique, gis):

    old_sdf, new_sdf = handle_service_conversion([old_url, new_url], gis)

    return geo_main(old_sdf, new_sdf, unique, gis)
#--------------------------------------------------------------------------

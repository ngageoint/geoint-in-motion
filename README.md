# geoint-in-motion
Has your data changed? If so, how? Has the attribution been enriched? Have geometries been added, removed or modified? The _GEOINT in Motion_ toolbox enables analysts to quickly understand where feature in their database have changed, either due to attribution changes or geometry changes, and what those changes were.

These tools have broad applications. They can be used to analyze changes to authoritative topographic features. They can similarly be used to identify how roads have changed in [OpenStreetMap](https://www.openstreetmap.org/) since the last time you downloaded OSM.

# Toolbox and Scripts
_GEOINT in Motion_ tools were written in Python and turned into an ArcGIS Toolbox that can be used in ArcGIS Pro 1.2+ or ArcGIS Desktop 10.4+. The toolbox contains the tools listed below. Each tool has a coresponding Python script that can be run as a stand-alone script.

![](http://nga.maps.arcgis.com/sharing/rest/content/items/049209394aee4fd2af72db5a1b331bf3/data)

## Attribute Change Ranking
Compares tables with the same name in a given polygon.  The tool will examin the total rows and compare the field's total differences and generate a set of rankings in that polygon grid for the fields. This tool should be used with feature data that does not contain unique object IDs.
## Basic Data Comparison
Compares two datasets that have the same schema based on a list of features within feature classes. Features and attributes that are being compared are taken from an excel spreadsheet.
## Compare Geometry By Unique ID
Takes two features classes as inputs and determines what features have been added, removed, or had their geometries modified. The input feature classes need to have a unique ID field in order to determine the additions, removals, and deletions.
## Compare Table Attributes By Unique ID
Takes two features classes as inputs and determines what features have had attributes modified in any way. The input feature classes need:
- A unique ID field in order to determine feature attribute changes.
- To be in the same schema (attribute fields must be identical).
## Sanitize Fields
Replaces a specific value within an attribute field with a user defined value.
## Spatial Change Ranking
Compares corresponding feature classes within different geodatabase. This is intended to compare snapshots of the same database taken at two different times. This tool should be used with feature data that does not contain unique object IDs.

# Installation and Use
The GEOINT in Motion tools use Python and Esri's arcpy library. In order for the tools to run, ArcGIS Desktop verision 10.4+ or ArcGIS Pro 1.2+ must be installed. If that condition is met, you should be able to clone this repo and run the tools as ArcGIS geoprocessing tools through ArcMap  or ArcGIS Pro or as stand alone scripts.

# Points of Contact
- Derek Silva (Derek.A.Silva@nga.mil)

# Developers
In aphabetical orther by last name:
- Gregory Brunner (gbrunner@esri.com)
- Andrew Chapkowski (achapkowski@esri.com)
- Todd Wever (twever@esri.com)

# Contributing

This tool was developed at the National Geospatial-Intelligence Agency (NGA) in collaboration with ESRI. The government has "unlimited rights" and is releasing this software to increase the impact of government investments by providing developers with the opportunity to take things in new directions. The software use, modification, and distribution rights are stipulated within the MIT license.

All pull request contributions to this project will be released under the MIT or compatible license. Software source code previously released under an open source license and then modified by NGA staff is considered a "joint work" (see 17 USC § 101); it is partially copyrighted, partially public domain, and as a whole is protected by the copyrights of the non-government authors and must be released according to the terms of the original open source license.

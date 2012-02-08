import os

import arcpy

def ascii2img(ascii, img=None, spatial_ref=None, overwrite=False):

    if not img:
        img = os.path.splitext(ascii)[0] + ".img"

    if not os.path.exists(img) or overwrite:
        print "\nConverting ASCII to IMG"
        print ascii + "\n" + img
        arcpy.ASCIIToRaster_conversion(ascii, img, "INTEGER")

    if spatial_ref:
        print "Defining spatial reference"
        arcpy.DefineProjection_management (img, spatial_ref)

    return img

def deleteFields(fname, delete=list(), keep=list(), test=False):

    print fname
    # just in case we pass a single path string instead of a list
    if type(delete) != list:
        delete = list(delete)
    if type(keep) != list:
        keep = [keep]

    # If we don't pass a list of fields to drop, delete everything but
    # OID and Shape.
    # want to keep OID, can't delete Shape

    a = [keep.append(i) for i in ["OID", "OBJECTID", "Shape", "FID"]]
    keep = list(set(keep))
    print "keep", keep

    if len(delete) == 0:

        # get name of all fields in shapefile
        fields = arcpy.ListFields(fname)

        # prep to drop all fields but the ones in keep
        delete = [field.name for field in fields if field.name not in keep]

    # len(delete_fields) raises TypeError if delete_fields == None
    if len(delete) > 0:
        print "\nDropping %s from %s" % (delete, fname)

        # self-explanatory function, no?
        # N.B. this deletes the fields in place - that is,
        # it doesn't create a new dataset so this doesn't preserve the
        # integrity of the original dataset.

        if not test:
            arcpy.DeleteField_management(fname, delete)
    else:
        print "No fields to delete from %s" % fname

    return fname

def clipRaster(in_raster, in_template_dataset, out_raster, overwrite=False):

    rectangle = "#"
    if not os.path.exists(out_raster) or overwrite:
        print "Clipping raster:"
        print "\n".join([in_raster, in_template_dataset, out_raster])

        arcpy.Clip_management(in_raster, rectangle, out_raster, in_template_dataset)

    return out_raster

def addFields(fname, field_name, dtype, test=False):

    fields = arcpy.ListFields(fname)

    # we only want to do this if the field doesn't exist yet
    if field_name not in fields:
        arcpy.AddField_management(fname, field_name, dtype)

    return fname

def sampleRaster(modis_points, rasters, out_table):

    print "\nSampling"
    print modis_points
    print rasters
    print out_table

    arcpy.sa.Sample(in_rasters=rasters, in_location_data=modis_points,
                    resampling_type="NEAREST", out_table=out_table)

    return out_table

def modisGadmIntersect(pts, gadm, outfile):

    arcpy.Intersect_analysis(in_features=[pts, gadm], out_feature_class=outfile)

    return modis_points

def bulkDeleteFields(file_paths, delete=list(), keep=list()):

    # file_paths is a dictionary of shapefile nicknames and paths

    # clean_paths is a dictionary of files whose fields have been dropped
    clean_paths = {}

    for ds in file_paths:

        # If we don't pass a list of fields to drop, delete everything but
        # OID and Shape.

        if not keep:
            # want to keep OID, can't delete Shape
            keep = ["OID", "OBJECTID", "Shape"]

        if not delete:

            # get name of all fields in shapefile
            fields = arcpy.ListFields(file_paths[ds])

            # prep to drop fields but the ones in keep_fields
            delete = [field.name for field in fields if field.name not in keep]

        # len(delete_fields) raises TypeError if delete_fields == None
        if len(delete) > 0:
            print "/nDropping %s from %s" % (delete, ds)

            # self-explanatory function, no?
            # N.B. this deletes the fields in place - that is,
            # it doesn't create a new dataset so this doesn't preserve the
            # integrity of the original dataset.

            arcpy.DeleteField_management(file_paths[ds], delete)

        else:
            print "No fields to delete from %s" % ds

            clean_paths[ds] = file_paths[ds]

    return clean_paths

def bulkMove():

    from forma_classes import FileObj, DateConverter
    import forma_init
    import forma_utils as utils

    aws, d = forma_init.init()
    dc = DateConverter()

    bucket = "modisfiles"
    aws = utils.s3_bucket_create(aws, bucket)
    dataset = "MOD13A3"
    prefix = "%s/%s" % (dataset, dataset)
    rs = aws.b.list(prefix)

    #n = 0

    for key in rs:
        f = FileObj()
        fname = key.name.split(prefix.split("/")[0] + "/")[1]
        f.remote = "s3://%s/%s/%s" % (bucket, dataset, fname)
        yyyy = fname[9:13]
        jjj = fname[13:16]
        yyyy, mm, dd = dc.jjj2date(yyyy, jjj)
        new_key = "s3://%s/%s/%i-%02i-%02i/%s" % (bucket, dataset, yyyy, mm, dd, fname)
        f.s3_move(new_key)
    #    n += 1
    #    if n == 000:
    #        break

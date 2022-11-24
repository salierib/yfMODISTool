# -- coding:cp936 �C
import arcpy
import time
import os


def find_tifs(in_dir):
    # ���ص�ǰ�ļ��У����������ļ��У�in_dir����չ��Ϊ.tif���ļ��ľ���·�����ɵ��б�
    return [os.path.join(in_dir, fname) for fname in os.listdir(in_dir) if fname.endswith(".tif")]


def batch_mkdir(workspace, names):
    """
    �ڵ�ǰ�ļ��������δ�����Ϊnames[i]�����ļ���,�������½��ļ��еľ���·��
    Parameters
    ----------
    workspace:str
        ��ǰ�ļ��еľ���·��
    names:list[str]
        ���ļ��е�����
    """
    paths = [os.path.join(workspace, name) for name in names]
    for p in paths:
        if not os.path.exists(p):
            os.mkdir(p)
    return paths


def localtime():
    # ���ص�ǰ��ʱ��
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())


def batch_extract_sds(hdfs, out_path, sds_index=0, suffix="NDVI"):
    """
    ������ȡ�����ݼ�����

    ����
    ----------
    hdfs:List[str]
        ��hdf�ļ��ľ���·����ɵ��б�
    out_path:str
        ��ȡ�����ݼ�����ļ���
    sds_index:int
        ����ȡ�������ݼ�����������0��ʼ
    suffix:str
        ��ȡ�����ݼ����������ļ����ĺ�׺��Ĭ��Ϊ"NDVI"
    """
    nums = len(hdfs)
    num = 1
    for hdf in hdfs:
        s = time.time()
        base_name = os.path.splitext(os.path.basename(hdf))[0]
        out_tif = os.path.join(out_path, base_name + "." + "{0}.tif".format(suffix))
        if not os.path.exists(out_tif):
            try:
                arcpy.ExtractSubDataset_management(hdf, out_tif, sds_index)
                e = time.time()
                arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, out_tif, e - s))
            except Exception as err:
                arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, out_tif, err))
        else:
            arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, out_tif))
        num += 1


def normal_mosaic_rule(fname):
    """
    ִ������ƴ�Ӳ����ķ�����򣬼����ں��ֹ���Բ���ƴ�Ӳ�����դ���ļ����з��飬����ͬһ���е�դ��ִ��ƴ�Ӳ���

    ����
    ----------
    fname:str
        �ļ���

    Returns
    -------
    ��ǰդ���ļ�����������

    """
    return '.'.join(fname.split('.')[:2]) + '.' + '.'.join(fname.split('.')[-2:])


def group_tifs(tif_names, group_func="mosaic"):
    """
    ����
    ----------
    tif_names:List[str]
        ���������ļ���
    group_func:str��or function
        �������������õ��ĺ���

    Returns
    -------
    grouped_tifs:dict
        ��Ϊ����,ֵΪ��ǰ�������դ���ļ����ļ������ɵ��б�
    """
    if group_func == "mosaic":
        group_func = normal_mosaic_rule
    grouped_tifs = {}
    for tif_name in tif_names:
        k = group_func(tif_name)
        if k in grouped_tifs:
            grouped_tifs[k].append(tif_name)
        else:
            grouped_tifs[k] = []
            grouped_tifs[k].append(tif_name)
    return grouped_tifs


def batch_mosaic(in_dir, out_path, groups=None, pixel_type="16_BIT_SIGNED", mosaic_method="MAXIMUM",
                 colormap_mode="FIRST"):
    """
    ����ƴ�ӹ���

    ��in_dir�е�����.tif�ļ�����groups�е��ļ������鷽ʽ��������Ƕ

    ����
    ----------
    in_dir��str
        �����ļ���
    out_path��str
        ����ļ���
    groups��dict
        ��Ϊ������ֵΪ��ǰ���Ӧ���ļ����б�
    pixel_type��str
        ָ�����դ�����ݼ���λ��ȣ�Ĭ��Ϊ"16_BIT_SIGNED"
    mosaic_method��str
        ������Ƕ�ص��ķ�����Ĭ��Ϊ"LAST"
    colormap_mode��str
        ������դ����Ӧ������Ƕ�����ɫ��ӳ������ѡ��ķ�����Ĭ��Ϊ"FIRST"
    """
    tif_names = [n for n in os.listdir(in_dir) if n.endswith(".tif")]
    if groups is None:
        groups = group_tifs(tif_names, group_func="mosaic")
    arcpy.env.workspace = in_dir
    base = tif_names[0]
    out_coor_system = arcpy.Describe(base).spatialReference
    cell_width = arcpy.Describe(base).meanCellWidth
    band_count = arcpy.Describe(base).bandCount
    nums = len(groups)
    num = 1
    for i in groups:
        s = time.time()
        groups[i] = ';'.join(groups[i])
        if not os.path.exists(os.path.join(out_path, i)):
            try:
                arcpy.MosaicToNewRaster_management(groups[i], out_path, i, out_coor_system, pixel_type,
                                                   cell_width,
                                                   band_count, mosaic_method, colormap_mode)
                e = time.time()
                arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, i, e - s))
            except Exception as err:
                arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, i, err))
        else:
            arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, i))
        num = num + 1


def batch_project_raster(rasters, out_path, prefix=None, out_coor_system="WGS_1984.prj",
                         resampling_type="NEAREST", cell_size="#"):
    """
    ����ͶӰդ�񹤾�

    ��in_dir�е�����դ���ļ�ͶӰ��out_corr_syestem����ϵ��

    Parameters
    ----------
    rasters:List[str]
        �ɴ�����ͶӰդ�������դ���ļ��ľ���·����ɵ��б�
    out_path:str
        ����ͶӰդ��������ļ���
    prefix:str
        ͶӰ��դ������ļ�����ǰ׺
    out_coor_system:
        ��ͶӰ����Ŀ������ϵ�ļ�·��(.prj)
    resampling_type:str
        Ҫʹ�õ��ز����㷨��Ĭ������Ϊ NEAREST��
        NEAREST �����ڽ����䷨
        BILINEAR ��˫���Բ�ֵ��
        CUBIC �����ξ����ֵ��
        MAJORITY �������ز�����
    cell_size:str
        ��դ�����ݼ�����Ԫ��С��
        ������ֱ���Ϊ250m����Ϊ��250 250"

    Examples
    ----------
    >> in_dir = r"S:\1_merge"
    >> tifs = [os.path.join(in_dir,n) for n in os.listdir(in_dir) if n.endswith(".tif")]
    >> batch_project_raster(tifs,  out_path=r"S:\test2")
    """
    if arcpy.CheckExtension("Spatial") != "Available":
        arcpy.AddMessage("Error!!! Spatial Analyst is unavailable")
    if prefix is None:
        prefix = ""
    nums = len(rasters)
    num = 1
    for raster in rasters:
        s = time.time()
        raster_name = os.path.split(raster)[1]
        out_raster = os.path.join(out_path, prefix + raster_name)
        if not os.path.exists(out_raster):
            try:
                arcpy.ProjectRaster_management(raster, out_raster, out_coor_system, resampling_type, cell_size,
                                               "#",
                                               "#", "#")
                e = time.time()
                arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, out_raster, e - s))
            except Exception as err:
                arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, out_raster, err))
        else:
            arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, raster))
        num = num + 1


def batch_clip_raster(rasters, out_path, masks):
    """
    �����ü�����

    Parameters
    ----------
    rasters:List[str]
        �ɴ����вü�������դ���ļ���ɵ��б�
    out_path:str
        �����ü��������ļ���
    masks:List
        ����Ϊ���з�Χʹ�õ�����դ���ʸ��ͼ��


    Examples
    ----------
    >> rasters = [u'H:\\NDVI_china\\scriptTest\\0_ndvi\\A2004001.NDVI.tif',
               u'H:\\NDVI_china\\scriptTest\\0_ndvi\\A2004032.NDVI.tif',
               u'H:\\NDVI_china\\scriptTest\\0_ndvi\\A2004061.NDVI.tif']
    >> masks = [u'H:\\NDVI_china\\scriptTest\\0_shapefiles\\anhui.shp',
                u'H:\\NDVI_china\\scriptTest\\0_shapefiles\\beijing.shp']
    >> batch_clip_raster(rasters=tifs,masks=masks,out_path=r"S:\test2")

    1/6 | anhui_A2004001.NDVI.tif Completed, time used 5.54600000381s
    2/6 | anhui_A2004032.NDVI.tif Completed, time used 0.138999938965s
    3/6 | anhui_A2004061.NDVI.tif Completed, time used 0.136000156403s
    4/6 | beijing_A2004001.NDVI.tif Completed, time used 0.105000019073s
    5/6 | beijing_A2004032.NDVI.tif Completed, time used 0.100999832153s
    6/6 | beijing_A2004061.NDVI.tif Completed, time used 0.101000070572s

    """
    nums = len(rasters) * len(masks)
    num = 1
    for i, mask in enumerate(masks):
        mask_name = os.path.splitext(os.path.basename(mask))[0]
        for raster in rasters:
            s = time.time()
            old_raster_name = os.path.splitext(os.path.basename(raster))[0]
            new_raster_name = "{0}_{1}.tif".format(mask_name, old_raster_name.split("_")[-1])
            out_raster = os.path.join(out_path, new_raster_name)
            if not os.path.exists(out_raster):
                try:
                    arcpy.Clip_management(raster, "#", out_raster, mask, "#", "ClippingGeometry")
                    e = time.time()
                    arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, out_raster, e - s))
                except Exception as err:
                    arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, out_raster, err))
            else:
                arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, out_raster))
            num += 1


def batch_multiply(rasters, out_path, scale_factor=0.0001, prefix=None):
    """
    �����˹���

    ������դ�����Ԫֵ������������

    Parameters
    ----------
    rasters:List[str]
        �ɴ����г˲�����դ���ļ���ɵ��б�
    out_path:str
        �����˺������ļ���
    scale_factor:float
        Ĭ��Ϊ0.0001����ӦNDVI
    prefix:str,optional
        ִ�г˲������ļ���ǰ׺��Ĭ��Ϊ��scaled_"

    Examples
    -------

    """
    arcpy.CheckOutExtension("Spatial")
    if arcpy.CheckExtension("Spatial") != "Available":
        arcpy.AddMessage("Error!!! Spatial Analyst is unavailable")
    if prefix is None:
        prefix = ""
    scale_factor = str(scale_factor)

    nums = len(rasters)
    num = 1
    for raster in rasters:
        s = time.time()
        raster_name = os.path.split(raster)[1]
        out_raster = os.path.join(out_path, prefix + raster_name)
        if not os.path.exists(out_raster):
            try:
                arcpy.gp.Times_sa(raster, scale_factor, out_raster)
                e = time.time()
                arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, out_raster, e - s))
            except Exception as err:
                arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, out_raster, err))
        else:
            arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, out_raster))
        num = num + 1


def batch_setnull(rasters, out_path, condition="VALUE>65528", prefix=None):
    """
    ������Ϊ�չ���

    Parameters
    ----------
    rasters:List[str]
        �ɴ�������Ϊ�ղ�����դ���ļ���ɵ��б�
    out_path:str
        ������Ϊ�պ������ļ���
    condition:List
        ����������ԪΪ���ٵ��߼����ʽ,Ĭ��Ϊ"VALUE>65528"
    prefix:str,optional
        ��Ϊ�պ����ļ���ǰ׺��Ĭ��Ϊ"sn_"
    """
    arcpy.CheckOutExtension("Spatial")
    if prefix is None:
        prefix = ""
    nums = len(rasters)
    num = 1
    for raster in rasters:
        s = time.time()
        raster_name = os.path.split(raster)[1]
        out_raster = os.path.join(out_path, prefix + raster_name)
        if not os.path.exists(out_raster):
            try:
                arcpy.gp.SetNull_sa(raster, raster, out_raster, condition)
                e = time.time()
                arcpy.AddMessage("%d/%d | %s completed, time used %.2fs" % (num, nums, out_raster, e - s))
            except Exception as err:
                arcpy.AddMessage("%d/%d | %s errored, %s" % (num, nums, out_raster, err))
        else:
            arcpy.AddMessage("%d/%d | %s already exists" % (num, nums, out_raster))
        num = num + 1


def mod13preprocess(workspace, hdfs, masks, out_coor_system, cell_size="#",
                    sds_index=0, sds_name="NDVI",
                    pixel_type="16_BIT_SIGNED", mosaic_method="LAST", colormap_mode="FIRST",
                    pr_prefix="pr_", resampling_type="NEAREST",
                    scale_prefix="", scale_factor=0.0001):
    tile_count = len(set([os.path.split(h)[1].split(".")[2] for h in hdfs]))
    if tile_count == 1:
        paths = batch_mkdir(workspace, ["1_extract", "2_reproject", "3_clip", "4_scale"])
        
        # step1
        s = time.time()
        arcpy.AddMessage("Starting step 1/4: extract subdataset into {0}... {1}".format(paths[0], localtime()))
        batch_extract_sds(hdfs, paths[0], sds_index=sds_index, suffix=sds_name)
        e = time.time()
        arcpy.AddMessage("Time for step1 = %.2fs. %s\n" % (e - s, localtime()))

        # step2
        s = time.time()
        arcpy.AddMessage("Starting step 2/4: reproject raster into {0}... {1}".format(paths[1], localtime()))
        batch_project_raster(find_tifs(paths[0]), paths[1], prefix=pr_prefix, out_coor_system=out_coor_system,
                             resampling_type=resampling_type, cell_size=cell_size)
        e = time.time()
        arcpy.AddMessage("Time for step2 = %.2fs. %s\n" % (e - s, localtime()))

        # step3
        s = time.time()
        arcpy.AddMessage("Starting step 3/4: clip raster into {0}... {1}".format(paths[2], localtime()))
        batch_clip_raster(find_tifs(paths[1]), paths[2], masks=masks)
        e = time.time()
        arcpy.AddMessage("Time for step3 = %.2fs. %s\n" % (e - s, localtime()))

        # step4
        s = time.time()
        arcpy.AddMessage("Starting step 4/4:raster times scale factor into {0}... {1}".format(paths[3], localtime()))
        batch_multiply(find_tifs(paths[2]), out_path=paths[3], prefix=scale_prefix, scale_factor=scale_factor)
        e = time.time()
        arcpy.AddMessage("Time for step4 = %.2fs. %s\n" % (e - s, localtime()))
    else:
        paths = batch_mkdir(workspace, ["1_extract", "2_mosaic", "3_reproject", "4_clip", "5_scale"])

        # step1
        s = time.time()
        arcpy.AddMessage("Starting step 1/5: extract subdataset into {0}... {1}".format(paths[0], localtime()))
        batch_extract_sds(hdfs, paths[0], sds_index=sds_index, suffix=sds_name)
        e = time.time()
        arcpy.AddMessage("Time for step1 = %.2fs. %s\n" % (e - s, localtime()))

        # step2
        s = time.time()
        arcpy.AddMessage("Starting step 2/5: mosaic raster into {0}... {1}".format(paths[1], localtime()))
        batch_mosaic(paths[0], paths[1], pixel_type=pixel_type, mosaic_method=mosaic_method,
                     colormap_mode=colormap_mode)
        e = time.time()
        arcpy.AddMessage("Time for step2 = %.2fs. %s\n" % (e - s, localtime()))

        # step3
        s = time.time()
        arcpy.AddMessage("Starting step 3/5: reproject raster into {0}... {1}".format(paths[2], localtime()))
        batch_project_raster(find_tifs(paths[1]), paths[2], prefix=pr_prefix, out_coor_system=out_coor_system,
                             resampling_type=resampling_type, cell_size=cell_size)
        e = time.time()
        arcpy.AddMessage("Time for step3 = %.2fs. %s\n" % (e - s, localtime()))

        # step4
        s = time.time()
        arcpy.AddMessage("Starting step 4/5: clip raster into {0}... {1}".format(paths[3], localtime()))
        batch_clip_raster(find_tifs(paths[2]), paths[3], masks=masks)
        e = time.time()
        arcpy.AddMessage("Time for step4 = %.2fs. %s\n" % (e - s, localtime()))

        # step5
        s = time.time()
        arcpy.AddMessage("Starting step 5/5:raster times scale factor into {0}... {1}".format(paths[4], localtime()))
        batch_multiply(find_tifs(paths[3]), out_path=paths[4], prefix=scale_prefix, scale_factor=scale_factor)
        e = time.time()
        arcpy.AddMessage("Time for step5 = %.2fs. %s\n" % (e - s, localtime()))


def mod16preprocess(workspace, hdfs, masks, out_coor_system, cell_size="#",
                    sds_index=0, sds_name="ET",
                    pixel_type="16_BIT_UNSIGNED", mosaic_method="LAST", colormap_mode="FIRST",
                    pr_prefix="pr_", resampling_type="NEAREST",
                    sn_prefix="sn_", condition="VALUE > 65528",
                    scale_prefix="", scale_factor=0.1):
    tile_count = len(set([os.path.split(h)[1].split(".")[2] for h in hdfs]))
    if tile_count == 1:
        paths = batch_mkdir(workspace, ["1_extract", "2_reproject", "3_clip", "4_setn", "5_scale"])
        # step1
        s = time.time()
        arcpy.AddMessage("Starting step 1/5: extract subdataset into {0}... {1}".format(paths[0], localtime()))
        batch_extract_sds(hdfs, paths[0], sds_index=sds_index, suffix=sds_name)
        e = time.time()
        arcpy.AddMessage("Time for step1 = %.2fs. %s\n" % (e - s, localtime()))

        # step2
        s = time.time()
        arcpy.AddMessage("Starting step 2/5: reproject raster into {0}... {1}".format(paths[1], localtime()))
        batch_project_raster(find_tifs(paths[0]), paths[1], prefix=pr_prefix, out_coor_system=out_coor_system,
                             resampling_type=resampling_type, cell_size=cell_size)
        e = time.time()
        arcpy.AddMessage("Time for step2 = %.2fs. %s\n" % (e - s, localtime()))

        # step3
        s = time.time()
        arcpy.AddMessage("Starting step 3/5: clip raster into {0}... {1}".format(paths[2], localtime()))
        batch_clip_raster(find_tifs(paths[1]), paths[2], masks=masks)
        e = time.time()
        arcpy.AddMessage("Time for step3 = %.2fs. %s\n" % (e - s, localtime()))

        # step4
        s = time.time()
        arcpy.AddMessage("Starting step 4/5: exclude invalid value, into {0}... {1}".format(paths[3], localtime()))
        batch_setnull(find_tifs(paths[2]), paths[3], condition=condition, prefix=sn_prefix)
        e = time.time()
        arcpy.AddMessage("Time for step4 = %.2fs. %s\n" % (e - s, localtime()))

        # step5
        s = time.time()
        arcpy.AddMessage("Starting step 5/5: raster times scale factor into {0}... {1}".format(paths[4], localtime()))
        batch_multiply(find_tifs(paths[3]), out_path=paths[4], prefix=scale_prefix, scale_factor=scale_factor)
        e = time.time()
        arcpy.AddMessage("Time for step5 = %.2fs. %s\n" % (e - s, localtime()))

    else:
        paths = batch_mkdir(workspace, ["1_extract", "2_mosaic", "3_reproject", "4_clip", "5_setn", "6_scale"])
        # step1
        s = time.time()
        arcpy.AddMessage("Starting step 1/6: extract subdataset into {0}... {1}".format(paths[0], localtime()))
        batch_extract_sds(hdfs, paths[0], sds_index=sds_index, suffix=sds_name)
        e = time.time()
        arcpy.AddMessage("Time for step1 = %.2fs. %s\n" % (e - s, localtime()))

        # step2
        s = time.time()
        arcpy.AddMessage("Starting step 2/6: mosaic raster into {0}... {1}".format(paths[1], localtime()))
        batch_mosaic(paths[0], paths[1], pixel_type=pixel_type, mosaic_method=mosaic_method,
                     colormap_mode=colormap_mode)
        e = time.time()
        arcpy.AddMessage("Time for step2 = %.2fs. %s\n" % (e - s, localtime()))

        # step3
        s = time.time()
        arcpy.AddMessage("Starting step 3/6: reproject raster into {0}... {1}".format(paths[2], localtime()))
        batch_project_raster(find_tifs(paths[1]), paths[2], prefix=pr_prefix, out_coor_system=out_coor_system,
                             resampling_type=resampling_type, cell_size=cell_size)
        e = time.time()
        arcpy.AddMessage("Time for step3 = %.2fs. %s\n" % (e - s, localtime()))

        # step4
        s = time.time()
        arcpy.AddMessage("Starting step 4/6: clip raster into {0}... {1}".format(paths[3], localtime()))
        batch_clip_raster(find_tifs(paths[2]), paths[3], masks=masks)
        e = time.time()
        arcpy.AddMessage("Time for step4 = %.2fs. %s\n" % (e - s, localtime()))

        # step5
        s = time.time()
        arcpy.AddMessage("Starting step 5/6: exclude invalid value, into {0}... {1}".format(paths[4], localtime()))
        batch_setnull(find_tifs(paths[3]), paths[4], condition=condition, prefix=sn_prefix)
        e = time.time()
        arcpy.AddMessage("Time for step5 = %.2fs. %s\n" % (e - s, localtime()))

        # step6
        s = time.time()
        arcpy.AddMessage("Starting step 6/6: raster times scale factor into {0}... {1}".format(paths[5], localtime()))
        batch_multiply(find_tifs(paths[4]), out_path=paths[5], prefix=scale_prefix, scale_factor=scale_factor)
        e = time.time()
        arcpy.AddMessage("Time for step5 = %.2fs. %s\n" % (e - s, localtime()))


class Toolbox(object):
    def __init__(self):
        """Define the toolbox (the name of the toolbox is the name of the
        .pyt file)."""
        self.label = "MODIS�ۺϴ�����"
        self.alias = ""

        # List of tool classes associated with this toolbox
        self.tools = [Tool1]


class Tool1(object):
    def __init__(self):
        """Define the tool (tool name is the name of the class)."""
        self.name = "MODIS�����ۺϴ���"
        self.label = "MODIS�����ۺϴ�����"
        self.description = """- �Ż��������鲻ִ����Ƕ����
                            - �޸�bug������ ��Ԫ��С �޸�Ϊ��ѡ
                            - ��ӣ���MOD15A2-LAI��֧��
                            - ��ӣ����ü���shp������ϵΪwgs 1984ʱ���Զ��趨Ŀ������ϵΪ��Ӧ��Χ��utm�ִ�"""
        self.canRunInBackground = False

    def getParameterInfo(self):
        """Define parameter definitions"""
        param_0 = arcpy.Parameter(displayName="Ԥ��", name="preset",
                                  datatype="String", parameterType="Required",
                                  direction="Input")
        param_0.filter.type = "ValueList"
        param_0.filter.list = ["MOD13_NDVI", "MOD13_EVI",
                               "MOD16_ET", "MOD16_PET",
                               "MOD11A2_LST",
                               "MOD17A2_GPP", "MOD17A3_NPP",
                               "MOD15A2_LAI",
                               "custom"]
        param_0.value = "MOD13_NDVI"
        param_1 = arcpy.Parameter(displayName="�����ռ�", name="ws",
                                  datatype="DEFolder", parameterType="Required",
                                  direction="Input")
        param_2 = arcpy.Parameter(displayName="hdf�ļ�", name="hdfs",
                                  datatype="DEFile", parameterType="Required",
                                  direction="Input", multiValue=True)
        param_2.filter.list = ["hdf"]
        param_3 = arcpy.Parameter(displayName="���ü��߽�", name="shps",
                                  datatype="DEFile", parameterType="Required",
                                  direction="Input", multiValue=True)
        param_3.filter.list = ["shp"]
        param_4 = arcpy.Parameter(displayName="Ŀ������ϵ", name="out_corr",
                                  datatype="GPSpatialReference", parameterType="Required",
                                  direction="Input")
        param_5 = arcpy.Parameter(displayName="��Ԫ��С", name="cell_size",
                                  datatype="GPCellSizeXY", parameterType="Optional",
                                  direction="Input")
        param_6 = arcpy.Parameter(displayName="�����ݼ�����", name="sds_idx",
                                  datatype="GPLong", parameterType="Optional",
                                  direction="Input")
        param_7 = arcpy.Parameter(displayName="�����ݼ�����", name="sds_name",
                                  datatype="GPString", parameterType="Required",
                                  direction="Input")
        param_8 = arcpy.Parameter(displayName="��������", name="pixel_type",
                                  datatype="GPString", parameterType="Required",
                                  direction="Input")
        param_8.filter.type = "ValueList"
        param_8.filter.list = ["1_BIT", "2_BIT", "4_BIT",
                               "8_BIT_UNSIGNED", "8_BIT_SIGNED", "16_BIT_UNSIGNED", "16_BIT_SIGNED",
                               "32_BIT_UNSIGNED", "32_BIT_SIGNED", "32_BIT_FLOAT", "64_BIT"]
        param_9 = arcpy.Parameter(displayName="��������", name="scale_factor",
                                  datatype="GPDouble", parameterType="Required",
                                  direction="Input")
        param_10 = arcpy.Parameter(displayName="��Ƕ�����", name="operator",
                                   datatype="GPString", parameterType="Required",
                                   direction="Input")
        param_10.filter.type = "ValueList"
        param_10.filter.list = ["FIRST", "LAST", "BLEND", "MEAN", "MINIMUM", "MAXIMUM", "SUM"]
        param_10.value = "MAXIMUM"
        param_11 = arcpy.Parameter(displayName="��Ƕɫ��ӳ���ģʽ", name="colormap",
                                   datatype="GPString", parameterType="Required",
                                   direction="Input")
        param_11.filter.type = "ValueList"
        param_11.filter.list = ["FIRST", "LAST", "MATCH", "REJECT"]
        param_11.value = "FIRST"
        param_12 = arcpy.Parameter(displayName="�ز�������", name="rs_method",
                                   datatype="GPString", parameterType="Required",
                                   direction="Input")
        param_12.filter.type = "ValueList"
        param_12.filter.list = ["NEAREST", "BILINEAR", "CUBIC", "MAJORITY"]
        param_12.value = "NEAREST"
        param_13 = arcpy.Parameter(displayName="�ļ���ǰ׺_ͶӰ", name="suffix_pr",
                                   datatype="GPString", parameterType="Optional",
                                   direction="Input")
        param_14 = arcpy.Parameter(displayName="�ļ���ǰ׺_����", name="suffix_scale",
                                   datatype="GPString", parameterType="Optional",
                                   direction="Input")
        param_15 = arcpy.Parameter(displayName="�ļ���ǰ׺_��Ϊ��", name="suffix_setn",
                                   datatype="GPString", parameterType="Optional",
                                   direction="Input")
        param_16 = arcpy.Parameter(displayName="ɸѡ����", name="con_filter",
                                   datatype="GPString", parameterType="Optional",
                                   direction="Input")
        params = [param_0, param_1, param_2, param_3, param_4, param_5, param_6,
                  param_7, param_8, param_9, param_10, param_11, param_12, param_13, param_14, param_15, param_16]
        for i in range(6, 17):
            params[i].category = "Advanced options"
        return params

    def initializeParameters(self, parameters):
        """Refine the properties of a tool's parameters.  This method is
        called when the tool is opened."""
        for i in range(6, 17):
            parameters[i].category = "Advanced options"
        return

    def isLicensed(self):
        """Set whether tool is licensed to execute."""
        return True

    def updateParameters(self, parameters):
        """Modify the values and properties of parameters before internal
        validation is performed.  This method is called whenever a parameter
        has been changed."""
        if parameters[0].altered:
            if parameters[0].value in ["MOD13_NDVI", "MOD13_EVI"]:
                parameters[16].enabled = 0
            else:
                parameters[16].enabled = 1
        if parameters[3].altered and parameters[3].value is not None:
            masks = parameters[3].valueAsText.split(";")
            if arcpy.Describe(masks[0]).spatialReference.factoryCode == 4326:
                xmin, xmax = arcpy.Describe(masks[0]).extent.XMin, arcpy.Describe(masks[0]).extent.XMax
                tar_utm = int((xmin + xmax) / 2 / 6) + 31
                if parameters[4].value is None:
                    parameters[4].value = "WGS 1984 UTM Zone %dN"%tar_utm
        if parameters[0].value == "MOD13_NDVI":
            # parameters[5].value = "250 250"  # cell_size
            parameters[6].value = 0  # sds_index
            parameters[7].value = "NDVI"  # sds_name
            parameters[8].value = "16_BIT_SIGNED"
            parameters[9].value = 0.0001
        elif parameters[0].value == "MOD13_EVI":
            # parameters[5].value = "250 250"  # cell_size
            parameters[6].value = 1  # sds_index
            parameters[7].value = "EVI"  # sds_name
            parameters[8].value = "16_BIT_SIGNED"
            parameters[9].value = 0.0001
        elif parameters[0].value == "MOD16_ET":
            # parameters[5].value = "500 500"  # cell_size
            parameters[6].value = 0  # sds_index
            parameters[7].value = "ET"  # sds_name
            parameters[8].value = "16_BIT_UNSIGNED"
            parameters[9].value = 0.1
            parameters[16].value = "VALUE > 32700"
        elif parameters[0].value == "MOD16_PET":
            # parameters[5].value = "500 500"  # cell_size
            parameters[6].value = 2  # sds_index
            parameters[7].value = "PET"  # sds_name
            parameters[8].value = "16_BIT_UNSIGNED"
            parameters[9].value = 0.1
            parameters[16].value = "VALUE > 32700"
        elif parameters[0].value == "MOD11A2_LST":
            parameters[5].value = "1000 1000"  # cell_size
            parameters[6].value = 0  # sds_index
            parameters[7].value = "LST"  # sds_name
            parameters[8].value = "16_BIT_UNSIGNED"
            parameters[9].value = 0.02
            parameters[16].value = "VALUE < 7500"
        elif parameters[0].value == "MOD17A2_GPP":
            parameters[5].value = "500 500"  # cell_size
            parameters[6].value = 0  # sds_index
            parameters[7].value = "GPP"  # sds_name
            parameters[8].value = "16_BIT_SIGNED"
            parameters[9].value = 0.0001
            parameters[16].value = "VALUE > 30000"
        elif parameters[0].value == "MOD17A3_NPP":
            parameters[5].value = "500 500"  # cell_size
            parameters[6].value = 0  # sds_index
            parameters[7].value = "NPP"  # sds_name
            parameters[8].value = "16_BIT_SIGNED"
            parameters[9].value = 0.0001
            parameters[16].value = "VALUE > 32700"
        elif parameters[0].value == "MOD15A2_LAI":
            parameters[5].value = "500 500"  # cell_size
            parameters[6].value = 1  # sds_index
            parameters[7].value = "LAI"  # sds_name
            parameters[8].value = "8_BIT_UNSIGNED"
            parameters[9].value = 0.1
            parameters[16].value = "VALUE > 100"
        return

    def updateMessages(self, parameters):
        """Modify the messages created by internal validation for each tool
        parameter.  This method is called after internal validation."""
        return

    def execute(self, parameters, messages):
        """The source code of the tool."""

        arcpy.env.parallelProcessingFactor = 0

        preset = parameters[0].valueAsText
        workspace = parameters[1].valueAsText
        hdfs = parameters[2].valueAsText
        masks = parameters[3].valueAsText
        out_coor_system = parameters[4].valueAsText
        cell_size = parameters[5].valueAsText
        sds_index = parameters[6].valueAsText
        sds_name = parameters[7].valueAsText
        pixel_type = parameters[8].valueAsText
        scale_factor = parameters[9].valueAsText
        mosaic_method = parameters[10].valueAsText
        colormap_mode = parameters[11].valueAsText
        resampling_type = parameters[12].valueAsText
        pr_prefix = parameters[13].valueAsText
        scale_prefix = parameters[14].valueAsText
        sn_prefix = parameters[15].valueAsText
        condition = parameters[16].valueAsText

        hdfs = hdfs.split(";")
        masks = masks.split(";")

        try:
            if preset in ["MOD13_NDVI", "MOD13_EVI"]:
                mod13preprocess(workspace=workspace,
                                hdfs=hdfs,
                                masks=masks,
                                out_coor_system=out_coor_system,
                                cell_size=cell_size,
                                sds_index=sds_index,
                                sds_name=sds_name,
                                pixel_type=pixel_type,
                                mosaic_method=mosaic_method,
                                colormap_mode=colormap_mode,
                                resampling_type=resampling_type,
                                scale_factor=scale_factor,
                                scale_prefix=scale_prefix,
                                pr_prefix=pr_prefix)
            elif preset in ["MOD16_ET", "MOD16_PET"]:
                mod16preprocess(workspace=workspace,
                                hdfs=hdfs,
                                masks=masks,
                                out_coor_system=out_coor_system,
                                cell_size=cell_size,
                                sds_index=sds_index,
                                sds_name=sds_name,
                                pixel_type=pixel_type,
                                mosaic_method=mosaic_method,
                                colormap_mode=colormap_mode,
                                resampling_type=resampling_type,
                                scale_factor=scale_factor,
                                scale_prefix=scale_prefix,
                                pr_prefix=pr_prefix,
                                sn_prefix=sn_prefix,
                                condition=condition)
            else:
                mod16preprocess(workspace=workspace,
                                hdfs=hdfs,
                                masks=masks,
                                out_coor_system=out_coor_system,
                                cell_size=cell_size,
                                sds_index=sds_index,
                                sds_name=sds_name,
                                pixel_type=pixel_type,
                                mosaic_method=mosaic_method,
                                colormap_mode=colormap_mode,
                                resampling_type=resampling_type,
                                scale_factor=scale_factor,
                                scale_prefix=scale_prefix,
                                pr_prefix=pr_prefix,
                                sn_prefix=sn_prefix,
                                condition=condition)
        except UnicodeEncodeError as cnerr:
            arcpy.AddMessage("�������󣡲��ֲ������������ַ����¸ù����޷����У��ɳ����޸�Ϊ��Ӣ���Խ��%s" % "".encode('utf-8'))
            raise cnerr
        return

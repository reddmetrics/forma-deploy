import getpass
import numpy as np

import machine_specs

bucket = "forma_test"

user = getpass.getuser()

paths = dict(danhammer = dict(code="/Users/%s/code/" % user,
                              data="/Users/%s/Desktop/ascii/" % user),

             robin     = dict(code="/Users/%s/code/" % user,
                              data="/Users/%s/Desktop/ascii/" % user),

             ubuntu    = dict(code="/home/%s/" % user,
                              data="/mnt/temp/"))
paths["Robin.Kraft"] = {"code":"/home/%s/" % user, "data":"/mnt/temp/"}

temp = paths[user]["data"]

dataset_dtypes = {'ndvi':np.int16,
                   'evi':np.int16,
                  'qual':np.uint16,
                  'reli':np.int8}

aws_access_key_id = "AKIAJ2R6HW5XJPY2GM3Q"
aws_secret_access_key = "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"

def modis_ftp():
    return "e4ftl01.cr.usgs.gov"

def getFiresParams():
    server = "mapsftp.geog.umd.edu"
    username = "public"
    password = "getfiredata"

    return server, username, password

paths = machine_specs.getPaths()

modis_products = {1000:{32:"MOD13A3.005", 16:"MOD13A2.005"},
                      500:{16:"MOD13A1.005"},
                      250:{16:"MOD13Q1.005"}}

tiles = ['0806', '0807', '0906', '0907', '0908', '0909', '1006', '1007', '1008',
         '1009', '1106', '1107', '1108', '1109', '1110', '1111', '1207',
         '1208', '1209', '1210', '1211', '1308', '1309', '1310', '1311', '1312',
         '1409', '1410', '1411', '1608', '1708', '1807', '1808', '1809', '1908',
         '1909', '2008', '2009', '2108', '2109', '2210', '2211', '2310', '2311',
         '2405', '2406', '2407', '2506', '2507', '2508', '2606', '2607', '2608',
         '2705', '2706', '2707', '2708', '2709', '2806', '2807', '2808', '2809', '2810',
         '2905', '2906', '2907', '2908', '2909', '3007', '3008', '3009', '3108',
         '3109', '3110', '3111', '3209', '3210', '3308', '3309', '3310']

tile_dict = {'AGO':['1909', '2009'],
             'ARG':['1111', '1211', '1311', '1312'],
             'AUS':['3110', '3111', '3210'],
             'BDI':['2009', '2109'],
             'BEN':['1807', '1808'],
             'BFA':['1708', '1807'],
             'BGD':['2506', '2606'],
             'BHS':['1006', '1106'],
             'BLZ':['0907'],
             'BOL':['1109', '1110', '1111', '1210', '1211'],
             'BRA':['1008', '1009', '1108', '1109', '1110', '1208', '1209', '1210', '1211', '1308', '1309', '1310', '1311', '1312', '1409', '1410', '1411'],
             'BTN':['2506', '2606'],
             'CAF':['1908', '2008'],
             'CHL':['1110', '1111'],
             'CHN':['2405', '2506', '2606', '2705', '2706', '2806', '2807'],
             'CIV':['1708'],
             'CMR':['1808', '1908'],
             'COD':['1908', '1909', '2008', '2009', '2108', '2109'],
             'COG':['1908', '1909'],
             'COL':['1007', '1008', '1009', '1107', '1108', '1109'],
             'COM':['2210'],
             'CRI':['0907', '0908'],
             'CUB':['1006', '1007', '1106', '1107'],
             'DOM':['1107'],
             'ECU':['0908', '0909', '1008', '1009'],
             'ETH':['2108'],
             'GAB':['1808', '1809', '1908', '1909'],
             'GHA':['1708', '1807', '1808'],
             'GIN':['1608', '1708'],
             'GLP':['1207'],
             'GNQ':['1808', '1908'],
             'GRD':['1107'],
             'GTM':['0907'],
             'GUF':['1208'],
             'GUY':['1108', '1208'],
             'HND':['0907'],
             'HTI':['1007', '1107'],
             'IDN':['2708', '2709', '2808', '2809', '2908', '2909', '3008', '3009', '3109', '3209'],
             'IND':['2405', '2406', '2407', '2506', '2507', '2508', '2606', '2607', '2707', '2708'],
             'JAM':['1007'],
             'JPN':['2905', '2906'],
             'KEN':['2108', '2109'],
             'KHM':['2707', '2807'],
             'KNA':['1107', '1207'],
             'LAO':['2706', '2707', '2807'],
             'LBR':['1608', '1708'],
             'LKA':['2508', '2608'],
             'MDG':['2210', '2211'],
             'MEX':['0806', '0807', '0906', '0907'],
             'MLI':['1807'],
             'MMR':['2606', '2607', '2706', '2707', '2708'],
             'MWI':['2109'],
             'MYS':['2708', '2808', '2908'],
             'NER':['1807'],
             'NGA':['1807', '1808', '1908'],
             'NIC':['0907'],
             'NPL':['2406', '2506'],
             'PAK':['2405', '2406'],
             'PAN':['0908', '1008'],
             'PER':['0909', '1009', '1109', '1110'],
             'PHL':['2907', '2908', '3007', '3008'],
             'PNG':['3109', '3209', '3210', '3309', '3310'],
             'PRI':['1107'],
             'PRK':['2705'],
             'PRY':['1210', '1211', '1311'],
             'REU':['2311'],
             'SDN':['2008', '2108'],
             'SLB':['3309', '3310'],
             'SLE':['1608'],
             'SLV':['0907'],
             'STP':['1808'],
             'SUR':['1208'],
             'TCD':['1908', '2008'],
             'TGO':['1807', '1808'],
             'THA':['2706', '2707', '2708', '2807', '2808'],
             'TTO':['1107', '1108', '1207'],
             'TWN':['2806', '2906'],
             'TZA':['2009', '2109'],
             'UGA':['2008', '2009', '2108', '2109'],
             'URY':['1311', '1312'],
             'USA':['0806', '0906', '1006', '1007'],
             'VEN':['1007', '1008', '1107', '1108', '1208'],
             'VIR':['1107'],
             'VNM':['2706', '2707', '2807', '2808'],
             'ZMB':['2009', '2109']}


def checktiles():
    tile_set = set()
    extra_tiles = set()
    tile_countries = {}
    missing = []
    
    # create a set of all tiles
    for iso, tile_list in tile_dict.items():
        for tile in tile_list:
            tile_set.add(tile)
            # track which countries go with which tiles
            # effectively the inverse of tile_dict
            try:
                tile_countries[tile].append(iso)
            except KeyError:
                tile_countries[tile] = []
                tile_countries[tile].append(iso)

    # look for any tiles from tile_set that don't appear in tiles
    # these are extras
    for tile in tiles:
        if tile not in tile_set:
            extra_tiles.add(tile)

    # ensure that all tiles in tile_dict appear in tiles (via tile_set)
    for tile in tile_set:
        if not tile in tiles:
            missing.append(tile)

    print tile_countries
    print "\nExtras:", extra_tiles
    print "\nMissing:", missing

    return tile_countries, extra_tiles, missing

#tile_set = set([tile for tile in tile_dict.values()].pop())
def tile_dictToClojureMap():
    maps = []
    dicts = []
    for iso, tiles in tile_dict.items():
        vals = []
        tiles = sorted(tiles)
        for tile in tiles:
            # turn each tile into a string like [28 8]
            vals.append("[%s]" % " ".join([str(int(tile[:2])), str(int(tile[2:]))]))

        # format vals as Clojure map with iso code
        mapified = ":%s #{%s}" % (iso, " ".join(vals))
        maps.append(mapified)

        dictified = "'%s':%s" % (iso, tiles)
        dicts.append(dictified)

    # create one big sorted map, nicely formatted for replacing Dan's Clojure map
    maps = sorted(maps)
    maps = "  {%s}" % "\n   ".join([m for m in maps])

    dicts = sorted(dicts)
    dicts = ",\n             ".join([d for d in dicts])
    print maps
    print dicts

    return

def reorderTile_dict():
    isos = {}
    for iso, tiles in tile_dict.items():
        tiles = sorted(tiles)
        isos[iso] = tiles

    sorted(isos)


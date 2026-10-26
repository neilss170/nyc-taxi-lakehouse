import io
import zipfile

import shapefile

from lakehouse import zonemap


def _zip(records):
    shp, shx, dbf = io.BytesIO(), io.BytesIO(), io.BytesIO()
    w = shapefile.Writer(shp=shp, shx=shx, dbf=dbf, shapeType=shapefile.POLYGON)
    w.field("LocationID", "N"); w.field("zone", "C"); w.field("borough", "C")
    for location_id, name, borough, ring in records:
        w.poly([ring]); w.record(location_id, name, borough)
    w.close()
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        for ext, buf in (("shp", shp), ("shx", shx), ("dbf", dbf)):
            z.writestr(f"taxi_zones.{ext}", buf.getvalue())
    return out.getvalue()


def test_simplify_drops_near_collinear_points_and_keeps_corners():
    line = [(0, 0), (50, 1), (100, 0), (100, 100)]
    assert zonemap.simplify(line, tolerance=5) == [(0, 0), (100, 0), (100, 100)]


def test_build_scales_flips_and_merges_split_zones():
    square = [(0, 0), (0, 1000), (1000, 1000), (1000, 0), (0, 0)]
    island = [(2000, 0), (2000, 1000), (3000, 1000), (3000, 0), (2000, 0)]
    shapes = zonemap.build(_zip([(56, "Corona", "Queens", square), (56, "Corona", "Queens", island)]))

    assert (shapes["width"], shapes["height"]) == (1000, 333)
    [zone] = shapes["zones"]
    assert (zone["location_id"], zone["zone"], zone["borough"]) == (56, "Corona", "Queens")
    assert zone["d"].count("M") == 2  # both records merged into one path
    # y is flipped: the shapefile's top edge (y=1000) lands at SVG y=0
    assert zone["d"].startswith("M0,333.3L0,0")

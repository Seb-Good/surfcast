                                    MAP FILE
readme.map.txt

map files:

erie5km.map       (5-km grid, 1015 water cells)
erie2km.map       (2-km grid, 6436 water cells)

huron.map         (15-km grid, 253 water cells)
huron5km.map      (5-km grid, 2374 water cells)
huron2km.map      (2-km grid, 14733 water cells)

michigan.map      (15-km grid, 253 water cells)
michigan5km.map   (5-km grid, 2318 water cells)
michigan2km.map   (2-km grid, 14458 water cells)

ontario.map       (10-km grid, 182 water cells)
ontario5km.map    (5-km grid, 746 water cells)

superior.map      (15-km grid, 354 water cells)
superior10km.map  (10-km grid, 807 water cells)

bd-100m.map       (100-m grid, 11982 water cells)
gh-100m.map       (100-m grid, 13133 water cells)
sb-200m.map       (200-m grid, 39045 water cells)


The ascii map file structure is:

NNNNN III JJJ LL.LLLLL LL.LLLLL DDD

where
NNNNN = sequence number
III = fortran column
JJJ = fortran row
LL.LLLLL = lat (decimal degrees N)
LL.LLLLL = lon (decimal degrees W)
DDD = depth (m)

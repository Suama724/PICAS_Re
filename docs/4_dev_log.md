3.25 state 1
先实现了数据基础类的存放
structure存论文人库等单元的结构
似乎还需要一套用来传输时的简化信息单子？

建了一个test区集中测每个阶段的东西

这个阶段需要测实例化是否正常等


doc table 暂时存一起了，到时候如果有切分必要再研究吧

有八百万个条目 doctable打不开了，还是要分段存

doctable 改成存bin后快乐很多，不知道原因有哪些，目前推测来自读写提升
但整个流程仍然用了69min

```record
=== T05 ===
xml_path=D:\Coding_Area\Python\DataStructureProj\proj_myself\datas\raw_data\dblp.xml
workspace=D:\Coding_Area\Python\DataStructureProj\proj_myself\datas\storage\bench\t05_full_dblp
sample_size_target=100
progress_interval=100000
[T05] imported=100000 elapsed=50.51s rate=1979.74 records/s
[T05] imported=200000 elapsed=101.51s rate=1970.27 records/s
[T05] imported=300000 elapsed=152.68s rate=1964.87 records/s
[T05] imported=400000 elapsed=204.00s rate=1960.82 records/s
[T05] imported=500000 elapsed=255.69s rate=1955.52 records/s
[T05] imported=600000 elapsed=306.58s rate=1957.10 records/s
[T05] imported=700000 elapsed=357.86s rate=1956.08 records/s
[T05] imported=800000 elapsed=408.79s rate=1957.01 records/s
[T05] imported=900000 elapsed=460.20s rate=1955.67 records/s
[T05] imported=1000000 elapsed=510.60s rate=1958.49 records/s
[T05] imported=1100000 elapsed=561.58s rate=1958.76 records/s
[T05] imported=1200000 elapsed=612.13s rate=1960.35 records/s
[T05] imported=1300000 elapsed=662.88s rate=1961.13 records/s
[T05] imported=1400000 elapsed=712.76s rate=1964.19 records/s
[T05] imported=1500000 elapsed=763.24s rate=1965.31 records/s
[T05] imported=1600000 elapsed=812.87s rate=1968.33 records/s
[T05] imported=1700000 elapsed=862.67s rate=1970.62 records/s
[T05] imported=1800000 elapsed=912.72s rate=1972.12 records/s
[T05] imported=1900000 elapsed=963.00s rate=1973.00 records/s
[T05] imported=2000000 elapsed=1014.39s rate=1971.62 records/s
[T05] imported=2100000 elapsed=1066.31s rate=1969.40 records/s
[T05] imported=2200000 elapsed=1117.42s rate=1968.82 records/s
[T05] imported=2300000 elapsed=1167.79s rate=1969.53 records/s
[T05] imported=2400000 elapsed=1218.94s rate=1968.92 records/s
[T05] imported=2500000 elapsed=1270.24s rate=1968.14 records/s
[T05] imported=2600000 elapsed=1320.89s rate=1968.36 records/s
[T05] imported=2700000 elapsed=1371.13s rate=1969.18 records/s
[T05] imported=2800000 elapsed=1421.79s rate=1969.35 records/s
[T05] imported=2900000 elapsed=1472.60s rate=1969.31 records/s
[T05] imported=3000000 elapsed=1522.81s rate=1970.04 records/s
[T05] imported=3100000 elapsed=1571.94s rate=1972.09 records/s
[T05] imported=3200000 elapsed=1621.28s rate=1973.75 records/s
[T05] imported=3300000 elapsed=1670.98s rate=1974.89 records/s
[T05] imported=3400000 elapsed=1720.30s rate=1976.40 records/s
[T05] imported=3500000 elapsed=1770.05s rate=1977.34 records/s
[T05] imported=3600000 elapsed=1820.08s rate=1977.93 records/s
[T05] imported=3700000 elapsed=1870.58s rate=1977.99 records/s
[T05] imported=3800000 elapsed=1921.26s rate=1977.87 records/s
[T05] imported=3900000 elapsed=1991.67s rate=1958.16 records/s
[T05] imported=4000000 elapsed=2042.00s rate=1958.87 records/s
[T05] imported=4100000 elapsed=2092.98s rate=1958.93 records/s
[T05] imported=4200000 elapsed=2142.71s rate=1960.14 records/s
[T05] imported=4300000 elapsed=2192.65s rate=1961.10 records/s
[T05] imported=4400000 elapsed=2242.86s rate=1961.78 records/s
[T05] imported=4500000 elapsed=2293.35s rate=1962.20 records/s
[T05] imported=4600000 elapsed=2342.30s rate=1963.88 records/s
[T05] imported=4700000 elapsed=2391.17s rate=1965.57 records/s
[T05] imported=4800000 elapsed=2440.38s rate=1966.91 records/s
[T05] imported=4900000 elapsed=2489.16s rate=1968.54 records/s
[T05] imported=5000000 elapsed=2537.86s rate=1970.17 records/s
[T05] imported=5100000 elapsed=2586.97s rate=1971.42 records/s
[T05] imported=5200000 elapsed=2638.05s rate=1971.16 records/s
[T05] imported=5300000 elapsed=2687.81s rate=1971.86 records/s
[T05] imported=5400000 elapsed=2737.56s rate=1972.56 records/s
[T05] imported=5500000 elapsed=2788.02s rate=1972.72 records/s
[T05] imported=5600000 elapsed=2838.01s rate=1973.22 records/s
[T05] imported=5700000 elapsed=2888.04s rate=1973.66 records/s
[T05] imported=5800000 elapsed=2937.89s rate=1974.21 records/s
[T05] imported=5900000 elapsed=2989.03s rate=1973.89 records/s
[T05] imported=6000000 elapsed=3039.15s rate=1974.24 records/s
[T05] imported=6100000 elapsed=3088.29s rate=1975.20 records/s
[T05] imported=6200000 elapsed=3136.91s rate=1976.47 records/s
[T05] imported=6300000 elapsed=3186.12s rate=1977.33 records/s
[T05] imported=6400000 elapsed=3236.34s rate=1977.54 records/s
[T05] imported=6500000 elapsed=3286.67s rate=1977.69 records/s
[T05] imported=6600000 elapsed=3336.00s rate=1978.42 records/s
[T05] imported=6700000 elapsed=3385.57s rate=1978.99 records/s
[T05] imported=6800000 elapsed=3434.76s rate=1979.76 records/s
[T05] imported=6900000 elapsed=3484.65s rate=1980.11 records/s
[T05] imported=7000000 elapsed=3534.31s rate=1980.59 records/s
[T05] imported=7100000 elapsed=3583.98s rate=1981.04 records/s
[T05] imported=7200000 elapsed=3633.66s rate=1981.48 records/s
[T05] imported=7300000 elapsed=3684.50s rate=1981.27 records/s
[T05] imported=7400000 elapsed=3735.60s rate=1980.94 records/s
[T05] imported=7500000 elapsed=3785.54s rate=1981.23 records/s
[T05] imported=7600000 elapsed=3835.51s rate=1981.48 records/s
[T05] imported=7700000 elapsed=3884.71s rate=1982.13 records/s
[T05] imported=7800000 elapsed=3934.11s rate=1982.66 records/s
[T05] imported=7900000 elapsed=3983.22s rate=1983.32 records/s
[T05] imported=8000000 elapsed=4031.92s rate=1984.17 records/s
[T05] imported=8100000 elapsed=4081.84s rate=1984.40 records/s
[T05] imported=8200000 elapsed=4128.62s rate=1986.14 records/s
total_records=8291540
elapsed_seconds=4170.17
records_per_second=1988.30
segment_count=6
segment_total_size_bytes=2924074516
segment_sizes_bytes=[536870855, 536870712, 536870838, 536870600, 536870775, 239720736]
doc_table_exists=True
manifest_exists=True
sample_size=100
sample_matches=100
sample_mismatches=0
```
目前这个doctable的逻辑还是整个加载到内存中

====

改成了随机读取的版本，应该爆内存的情况被避开了，整套流程目前正常

===

学到的是可以像doctable一样将具体数据存到bin里面，而再建立一层索引放偏移与具体参数
也就是原来 index - ids的映射改成
index - (offset count)的映射
然后输入的str先hash查桶，再在桶内遍历查key对应的键，
然后再去posting里面拿真的数据
这样posting开的很大也没关系，内存占用小
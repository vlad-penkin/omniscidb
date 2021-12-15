#!/localdisk2/gnovichk/miniconda3/envs/omnisci-dev/bin/python
from datetime import datetime

import numpy as np
import pandas
import pyarrow
import statistics
import sys
import time

prev = sys.getdlopenflags()
sys.setdlopenflags(1 | 256) # RTLD_LAZY+RTLD_GLOBAL

from omniscidbe import PyDbEngine

sys.setdlopenflags(prev)

engine = PyDbEngine(data="omnitmp", enable_columnar_output=1, enable_lazy_fetch=0, enable_debug_timer=1, calcite_port=4564)

def build_table(N: int = int(30*1E6), fragment_size: int = 0):
    df = pandas.DataFrame({"a": [4]*N})
    at = pyarrow.Table.from_pandas(df)
    engine.importArrowTable("test", at, fragment_size=fragment_size)

def drop_table():
    engine.executeDDL("DROP TABLE test;")

def profile_get_arrow_table(cursor):
    start_time = time.time()
    res = cursor.getArrowTable()
    return time.time() - start_time

def profile_get_arrow_record_batch(cursor):
    start_time = time.time()
    res = cursor.getArrowRecordBatch()
    return time.time() - start_time

def profile():
    cursor = engine.executeDML("SELECT a from test;")
    elapsed1 = profile_get_arrow_table(cursor)
    elapsed2 = profile_get_arrow_record_batch(cursor)
    return elapsed1,elapsed2

def dry_profile():
    return 0,0

def run_major_profiling():
    timestamp = datetime.now().strftime("%Y%m%d_%H:%M:%S")

    df = pandas.DataFrame(columns=["fragments_count", "fragment_size","getArrowTable.mean","getArrowTable.stdev",
                                "getArrowRecordBatch.mean", "getArrowRecordBatch.stdev",
                                "speedup.mean", "speedup.stdev", "speedup.approx"])

    N = int(30*1E6)
    base_cores_count = 28
    #fragments_count = [base_cores_count*i for i in range(1,101)]
    fragments_count = [1,2,3,4,5,6,7,8,9,10,20,30,40,50,60,70,80,90,100,150,200,250,300]

    #frag_sizes = [1000, 2000, 3000, 4000, 8000, 16000, 32000, 64000, 128000, 256000, 512000]

    for fc in fragments_count:
        fs = int (N/fc)
        print("Profiling; fragment count: %d, fragment size: %d"%(fc,fs))
        sys.stdout.flush()

        build_table(N, fs)

        elapsed1=[]
        elapsed2=[]
        for _ in range(0,100):
            el1, el2 = profile()
            elapsed1.append(el1)
            elapsed2.append(el2)

        drop_table()  
        
        el1=statistics.mean(elapsed1)
        el2=statistics.mean(elapsed2)
        std_el1=statistics.stdev(elapsed1)
        std_el2=statistics.stdev(elapsed2)

        speedups=np.array(elapsed2)/(np.array(elapsed1)+(1E-12))
        speedup_mean=statistics.mean(speedups)
        speedup_stdev=statistics.stdev(speedups)
        speedup_approx = el2/el1

        df = df.append({"fragments_count": fc, "fragment_size": fs, 
                        "getArrowTable.mean":el1, "getArrowTable.stdev":std_el1, 
                        "getArrowRecordBatch.mean":el2, "getArrowRecordBatch.stdev":std_el2,
                        "speedup.mean":speedup_mean, "speedup.stdev": speedup_stdev,
                        "speedup.approx": speedup_approx
                        }, ignore_index=True)
        print (std_el1/el1, std_el2/el2)
        sys.stdout.flush()

    df=df.set_index("fragments_count")
    print(df)
    output_csv_file="PROFILING-RUN_N"+str(N)+"_"+timestamp+".csv"

    df.to_csv(output_csv_file)
    print("## SAVED TO: %s"%output_csv_file)


def simple_invocation(N: int = int(30*1E6), fragments_count: int = 1):
    fragment_size = int (N/fragments_count)
    build_table(N, fragment_size)
    cursor = engine.executeDML("SELECT a from test;")
    print("Running SELECT a from test;")
    for _ in range(0,1):
        print("Running getArrowTable()")
        res = cursor.getArrowTable()
    drop_table() 

if __name__ == '__main__':
    # run_major_profiling()
    simple_invocation()

# amplxe-cl -collect hotspots -call-stack-mode=all -d 60 -search-dir=/nfs/site/home/gnovichk/shm/omniscidb/build/bin -source-search-dir=/localdisk/ilyaenko/omnisci -target-pid=`cat /nfs/site/home/gnovichk/shm/omniscidb/build/bin/data/omnisci_server_pid.lck` -user-data-dir=/nfs/site/home/gnovichk/shm/omniscidb/omnisci-profiles
# vtune -collect hotspots -knob sampling-mode=hw -knob enable-stack-collection=true -knob stack-size=1024 -knob enable-characterization-insights=true -start-paused -- /nfs/site/home/gnovichk/shm/omniscidb/build/bin/omnisci_server --data=/data/datac/ilyaenko/omniscidb/build/bin/data --db-query-list=tpc-h-q5.sql --enable-modern-thread-pool=true --max-num-threads=112 --intel-jit-profile --exit-after-warmup

# vtune -collect hotspots -knob sampling-mode=hw -knob enable-stack-collection=true -knob stack-size=1024 -knob enable-characterization-insights=true -start-paused -- ./rp.sh

# elapsed1,elapsed2 = profile()
# drop_table()

# print("getArrowTable():       Elapsed time, sec: %f (size: %d)"%(elapsed1, N))
# print("getArrowRecordBatch(): Elapsed time, sec: %f (size: %d)"%(elapsed2, N))
# print("Speedup: %f"%(elapsed2/elapsed1))

# print("### FINISHED ###")



"""
print("getArrowTable():       Elapsed time, sec: %f (size: %d)"%(elapsed1, N))
$ grep dura omnitmp/mapd_log/omnisci_dbe.INFO
178ms total duration for executeRelAlgQuery
1825ms total duration for sql_execute
6038ms total duration for convertToArrowTable
"""


"""
print("getArrowRecordBatch(): Elapsed time, sec: %f (size: %d)"%(elapsed2, N))
$ grep dura omnitmp/mapd_log/omnisci_dbe.INFO
153ms total duration for executeRelAlgQuery
1724ms total duration for sql_execute
247ms total duration for convertToArrow
"""


"""
print(N)
df = pandas.DataFrame({"a": [4]*5, "b": [2]*5})
at = pyarrow.Table.from_pandas(df)
engine.consumeArrowTable("t1", at, fragment_size=2)
cursor = engine.executeDML("SELECT a from t1;")
#cursor = engine.executeDML("SELECT a / b from t1;")
if hasattr(cursor, 'getArrowTable'):
    print ("O, there's getArrowTable!")
    res = cursor.getArrowTable()
    print(res.to_pandas())

if hasattr(cursor, 'getArrowRecordBatch'):
    print ("O, there's getArrowRecordBatch!")
    cursor.getArrowRecordBatch
    res = cursor.getArrowRecordBatch()
    print(res.to_pandas())
"""

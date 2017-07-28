"""Test performance and correct of gbucket storage plugin.

Requirements:
    * DVID server must be running on 127.0.0.1:8000 point to a google bucket
    * A bucket can be reused for many tests but a new bucket is required if the gbucket
    storage version is new.
    * libdvid library should be installed
    * Assumes emdata.janelia.org is running

Output: a file with test runtimes and success/fail messages
"""

from libdvid import DVIDNodeService, DVIDServerService, ConnectionMethod
import time
import requests
import numpy
import json

DVIDLOCATION = "127.0.0.1:8000"

""" Prints the time and result of the benchmark, terminates early if fails.

This decorator expects that the benchmark returns a status and a value that
can be used by the main function or which supplies an error message if necessary.
"""
def benchmark(bench):
    def wrapper(*args, **kwargs):
        start = time.time()
        if len(args) == 0 and len(kwargs) == 0:
            status, res = bench()
        else:
            status, res = bench(*args, **kwargs)
        finish = time.time()

        print "##### " + bench.__name__ + " #####"
        print bench.__doc__
        print "Runtime: " + str(finish-start) +  " seconds"
        if status:
            print "SUCCESS"
            print "#######################\n\n"
        else:
            print "FAILED:", res 
            print "#######################\n\n"
            # perform early exit
            exit(0)
        return status, res
    return wrapper

# helper function to load data
def loaddata(ns, name, data, offsetzyx):
    isgray = False
    if data.dtype == numpy.uint8:
        isgray = True
    
    try:
        if isgray:
            ns.put_gray3D(name, data, offsetzyx, throttle=False)
        else:
            ns.put_labels3D(name, data, offsetzyx, throttle=False)
    except Exception as e:
        return False, str(e)
    return True, None 


# helper function to check data loaded at an offset
def checkdata(ns, name, data, offsetzyx):
    isgray = False
    if data.dtype == numpy.uint8:
        isgray = True
    
    try:
        if isgray:
            data2 = ns.get_gray3D(name, data.shape, offsetzyx)
        else:
            data2 = ns.get_labels3D(name, data.shape, offsetzyx)
    except Exception as e:
        return False, str(e)

    if not numpy.array_equal(data, data2):
        return False, "Arrays not equal"
    return True, None

# helper function to check key value
def check_keyvalue(ns, name, key, val):
    try:
        val2 = ns.get(name, key)
    except Exception as e:
        return False, str(e)
    if val != val2:
        return False, "Values not equal"
    return True, None

# helper function to check key value
def check_keys(uuid, name, keys):
    try:
        req = requests.get("http://" + DVIDLOCATION + "/api/node/" + uuid + "/" + name + "/keys")
        keys2 = req.json()
    except Exception as e:
        return False, str(e)
    keys2.sort()
    if keys != keys2:
        return False, "Key lists are not equal"
    return True, None

def createrepo():
    """Creates a repo which results in a new gbucket.
    """
    try:
        ds = DVIDServerService(DVIDLOCATION)
        uuid = ds.create_new_repo("test", "test description")
    except Exception as e:
        return False, str(e)
    return True, uuid 
    
def load_data(task):
    z,y,x,uuid3,data,name = task
    nst = DVIDNodeService(DVIDLOCATION, uuid3)
    datanew = data[z:z+32,y:y+32,x:x+32].copy()
    loaddata(nst, name, datanew, (1321+z,1321+y,1321+x))

def volumetests(ns, uuid, name, data):
    """Checks get/put operations for the provided data.
    """

    @benchmark
    def bench_load_gbucket_repo():
        """Measure time it takes to init a new gbucket repo (done on first request).
        """
        isgray = False
        if data.dtype == numpy.uint8:
            isgray = True
        
        try:
            if isgray:
                data2 = ns.get_gray3D(name, (1,1,1), (0,0,0))
            else:
                data2 = ns.get_labels3D(name, (1,1,1), (0,0,0))
        except Exception as e:
            return False, str(e)
        return True, ""
    bench_load_gbucket_repo()

    @benchmark
    def bench_initialize_data():
        """Load cube at 0,0,0
        """
        return loaddata(ns, name, data, (0,0,0))
    bench_initialize_data()

    @benchmark
    def bench_get_initial_data():
        """Get cube at 0,0,0.
        """
        return checkdata(ns, name, data, (0,0,0))
    bench_get_initial_data()

    # commit node, create new version
    ns.custom_request("/commit", json.dumps({"note": "committed"}), ConnectionMethod.POST)
    res = ns.custom_request("/newversion", "", ConnectionMethod.POST)
    uuid2 = str(json.loads(res)["child"])

    ns2 = DVIDNodeService(DVIDLOCATION, uuid2)
    @benchmark
    def bench_put_versioned_data_offset():
        """Put disjoint cube from initial data (should be fast).
        """
        return loaddata(ns2, name, data, (512,512,512))
    bench_put_versioned_data_offset()

    @benchmark
    def bench_get_versioned_data_offset():
        """Get cube at offset (should be fast).
        """
        return checkdata(ns2, name, data, (512,512,512))
    bench_get_versioned_data_offset()

    # modify data for overwrite
    data2 = data.copy()
    data2[0:10,20:30,30:50] = 55

    @benchmark
    def bench_put_versioned_data_overwrite():
        """Put cube from initial data (should be 5x slower).
        """
        return loaddata(ns2, name, data2, (0,0,0))
    bench_put_versioned_data_overwrite()

    @benchmark
    def bench_get_versioned_data_overwrite():
        """Get overwritten data (should be fast).
        """
        return checkdata(ns2, name, data2, (0,0,0))
    bench_get_versioned_data_overwrite()

    # create branch off master
    res = ns.custom_request("/branch", json.dumps({"branch": "mybranch"}), ConnectionMethod.POST)
    uuid3 = str(json.loads(res)["child"])

    ns3 = DVIDNodeService(DVIDLOCATION, uuid3)
    @benchmark
    def bench_get_data_branch():
        """Get data from a branch.
        """
        return checkdata(ns3, name, data, (0,0,0))
    bench_get_data_branch()

    @benchmark
    def bench_put_branch_data_offset():
        """Put disjoint cube from initial data (should be fast).
        """
        return loaddata(ns3, name, data, (1024,1024,1024))
    bench_put_branch_data_offset()

    @benchmark
    def bench_get_branch_data_offset():
        """Get branch cube at offset (should be fast).
        """
        return checkdata(ns3, name, data, (1024,1024,1024))
    bench_get_branch_data_offset()

    @benchmark
    def bench_overwrite_branch():
        """Overwrite branch data with master version (should be 5x slower).
        """
        return loaddata(ns2, name, data2, (1024,1024,1024))
    bench_overwrite_branch()

    @benchmark
    def bench_get_overwrite_branch():
        """Get branch overwritten data from master (should be fast).
        """
        return checkdata(ns2, name, data2, (1024,1024,1024))
    bench_get_overwrite_branch()

    @benchmark
    def bench_get_overwrite_branch2():
        """Re-get branch data (slow). 
        """
        return checkdata(ns3, name, data, (1024,1024,1024))
    bench_get_overwrite_branch2()

    @benchmark
    def bench_overwrite_branch_shifted():
        """Overwrite shifted data on branch (slow).
        """
        return loaddata(ns3, name, data, (1088,1088,1088))
    bench_overwrite_branch_shifted()

    @benchmark
    def bench_get_versioned_again():
        """Get data again from new version should be unchanged (fast)
        """
        return checkdata(ns2, name, data2, (1024,1024,1024))
    bench_get_versioned_again()

    @benchmark
    def bench_get_branch_shifted():
        """Get branch data that was partially overwritten in place (slow)
        """
        return checkdata(ns3, name, data, (1088,1088,1088))
    bench_get_branch_shifted()

    @benchmark
    def bench_get_original_master():
        """Make sure versioned master is the same (slow)
        """
        return checkdata(ns, name, data, (0,0,0))
    bench_get_original_master()

    @benchmark
    def bench_put_nonaligned():
        """Load data in parallel and non-block aligned on branch (very slow).
        """
        from multiprocessing import Pool as ThreadPool
        pool = ThreadPool(16)

        # create pieces
        zsize,ysize,xsize = data.shape
        shift_queue = []
        for z in range(0, zsize, 32):
            for y in range(0, ysize, 32):
                for x in range(0, xsize, 32):
                    shift_queue.append((z,y,x,uuid3,data,name))

        pool.map(load_data, shift_queue)
        pool.close()
        pool.join()
        return True, ""
    bench_put_nonaligned()

    @benchmark
    def bench_get_nonaligned():
        """Get non-aligned branch data (slow)
        """
        return checkdata(ns3, name, data, (1321,1321,1321))
    bench_get_nonaligned()

    @benchmark
    def bench_deletedatatype():
        """Use command line to delete a datatype.  Non-blocking (test not too useful).
        """
        isgray = False
        if data.dtype == numpy.uint8:
            isgray = True
 
        try:
            import os
            os.system("dvid -rpc=127.0.0.1:8001 repo " + uuid + " delete " + name)
        except Exception as e:
            return False, str(e)
        try:
            if isgray:
                ns.get_gray3D(name, (10,10,10), (0,0,0))
            else:
                ns.get_label3D(name, (10,10,10), (0,0,0))
        except Exception as e:
            return True, None
        return False, "data info still exists"
    bench_deletedatatype()
      
    
# load data from emdata.janelia.org 7 column medulla
ns = DVIDNodeService("emdata.janelia.org", "8225")
grayscale = ns.get_gray3D("grayscale", (128,128,128), (4800, 3000, 3500))
labels = ns.get_labels3D("groundtruth", (128,128,128), (4800, 3000, 3500))

#grayscale = ns.get_gray3D("grayscale", (32,32,32), (4800, 3000, 3500))
#labels = ns.get_labels3D("groundtruth", (32,32,32), (4800, 3000, 3500))

# run benchmarks

@benchmark
def bench_createrepo():
    """Create new dvid repo.
    """
    return createrepo()
res, uuid = bench_createrepo()

ns = DVIDNodeService(DVIDLOCATION, uuid)
ns.create_grayscale8("grayscale")

@benchmark
def bench_reloadmeta(uuid):
    """Call dvid meta refresh.
    """
    try:
        res = requests.post("http://" + DVIDLOCATION + "/api/server/reload-metadata")
    except Exception as e:
        return False, str(e)
    
    if res.status_code != 200:
        return False, "reload command failed"
    return True, None
bench_reloadmeta(uuid)

# run tests on grayscale interface (will delete when finished)
print "*************Testing Grayscale*************\n"
volumetests(ns, uuid, "grayscale", grayscale)
print  "************End Testing Grayscale**********\n\n\n"


print "*************Testing Labels*************\n\n\n"
# create new repo for convenience
res, uuid = createrepo()
ns = DVIDNodeService(DVIDLOCATION, uuid)
ns.create_labelblk("labels")
# run tests on labelblk interface (will delete when finished)
volumetests(ns, uuid, "labels", labels)
print  "************End Testing Labels**********\n\n\n"

# TODO run tests on labelk array interface

# Test key value
print "*************Testing KeyValue*************\n\n\n"
res, uuid = createrepo()
ns = DVIDNodeService(DVIDLOCATION, uuid)
name = "files"
ns.create_keyvalue(name)

# add files to master
ns.put(name, "m1", "data1")
ns.put(name, "m2", "data2")
ns.put(name, "m3", "data3")

# check keys and a value
@benchmark
def bench_checkmasterkv():
    """Checks whether a keyvalue at master matches what was posted.
    """
    return check_keyvalue(ns, name, "m2", "data2")
bench_checkmasterkv()

@benchmark
def bench_checkmasterkeys():
    """Checks master range query.
    """
    keys = ["m1", "m2", "m3"]
    keys.sort()
    return check_keys(uuid, name, keys)
bench_checkmasterkeys()

# commit node, create new version
ns.custom_request("/commit", json.dumps({"note": "committed"}), ConnectionMethod.POST)
res = ns.custom_request("/newversion", "", ConnectionMethod.POST)
uuid2 = str(json.loads(res)["child"])
ns2 = DVIDNodeService(DVIDLOCATION, uuid2)

# add files to version
ns2.put(name, "v1", "vdata1")
ns2.put(name, "v2", "vdata2")
ns2.put(name, "m2", "vovewrite")

# check keys and a value
@benchmark
def bench_checkversionkeys():
    """Checks version range query.
    """
    keys = ["m1", "m2", "m3", "v1", "v2"]
    keys.sort()
    return check_keys(uuid2, name, keys)
bench_checkversionkeys()


# create branch off master
res = ns.custom_request("/branch", json.dumps({"branch": "mybranch"}), ConnectionMethod.POST)
uuid3 = str(json.loads(res)["child"])
ns3 = DVIDNodeService(DVIDLOCATION, uuid3)

# add files to version
ns3.put(name, "b1", "bdata1")
ns3.put(name, "b2", "bdata2")
ns3.put(name, "m2", "movewrite")

# check keys and a value
@benchmark
def bench_checkbranchkeys():
    """Checks branch range query.
    """
    keys = ["m1", "m2", "m3", "b1", "b2"]
    keys.sort()
    return check_keys(uuid3, name, keys)
bench_checkbranchkeys()

# Gest tombstone deletion 
@benchmark
def bench_delete_version():
    """Delete two version keys.
    """
    # perform delete of m2, v1
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid2 + "/" + name + "/key/m1")
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid2 + "/" + name + "/key/m2")
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid2 + "/" + name + "/key/v1")
    return True, ""
bench_delete_version()

@benchmark
def bench_delete_branch():
    """Delete three branch keys.
    """
    # perform delete of m2, m3, b2
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid3 + "/" + name + "/key/m2")
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid3 + "/" + name + "/key/m3")
    requests.delete("http://" + DVIDLOCATION + "/api/node/" + uuid3 + "/" + name + "/key/b2")
    return True, ""
bench_delete_branch()

@benchmark
def bench_request_deleted_data():
    """Requests data that should be deleted.
    """
    try:
        ns3.get(name, "m2")
    except Exception:
        return True, None
    return False, "Data still exists"
bench_request_deleted_data()

@benchmark
def bench_checkversionkeys2():
    """Checks version range query.
    """
    keys = ["m3", "v2"]
    keys.sort()
    return check_keys(uuid2, name, keys)
bench_checkversionkeys2()

@benchmark
def bench_checkbranchkeys2():
    """Checks version range query.
    """
    keys = ["m1", "b1"]
    keys.sort()
    return check_keys(uuid3, name, keys)
bench_checkbranchkeys2()

# recheck master
@benchmark
def bench_checkmasterkeys2():
    """Recheck master range query.
    """
    keys = ["m1", "m2", "m3"]
    keys.sort()
    return check_keys(uuid, name, keys)
bench_checkmasterkeys2()

# replace value on branch
ns3.put(name, "m2", "newdata")

@benchmark
def bench_checkbranch_undelete():
    """Retrieves a value that ovewrites a previous deletion.
    """
    return check_keyvalue(ns3, name, "m2", "newdata")
bench_checkbranch_undelete()

print  "************End Testing KeyValue**********\n\n\n"

@benchmark
def bench_deleterepo(uuid):
    """Use command line to delete a repo.  Non-blocking (test not too useful).
    """
    try:
        import os
        os.system("dvid -rpc=127.0.0.1:8001 repos delete " + uuid)
    except Exception as e:
        return False, str(e)
    
    try:
        ns = DVIDNodeService(DVIDLOCATION, uuid)
    except Exception as e:
        return True, None
    return False, "repo info still exists"
bench_deleterepo(uuid)




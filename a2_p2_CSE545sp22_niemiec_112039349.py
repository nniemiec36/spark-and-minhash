# Name: Nicole Niemiec
# SBID: 112039349
# Similar Hospital Search
# TASK II
import sys
from pyspark import SparkContext
import pprint
import random
import math

pp = pprint.PrettyPrinter(width=45, compact=True)
##########################################################################
##########################################################################
# TASK II.A: Extract binary features (i.e. a sparse representation of the characteristic matrix) per hospital
# such that:
# (131312, set('collection_week:11/5/2021', 'state:ID', 'hospital_name:ST LUKE'S MCCALL', 'fips_code:16085', 'is_metro:FALSE', 'beds_used_avg: 4.3'))
# (50739, set('collection_week:4/16/2021', 'state:CA', 'hospital_name:CENTINELA HOSPITAL MEDICAL CENTER', 'fips_code:6037','is_metro:TRUE', 'beds_used_avg:276.7))

def create_set(rdd, hospital_bc): # need this to be a spark thingy not a loop maybe map ? 
    empty_list = []
    rddlist = rdd[1:] # removing 'ID'
    
    # bc is a list of a list
    for x, y in zip(rdd, hospital_bc.value[0]):
        empty_list.append(y+":"+x)
    return set(empty_list)

def task2A(hospitals_rdd):
    print("##### TASK 2A #####")
    hospitals_rdd=hospitals_rdd.mapPartitions(lambda y: map(lambda line: line.split(","), y)) # they're lines with features now

    # saves the header in a broadcast variable
    hospital_bc = sc.broadcast(hospitals_rdd.take(1)) # first line, don't take the first index [don't need id number label]
    # print(hospital_bc.value)
    # header_row = hospitals_rdd.first() # all of the first line
    hospitals_rdd = hospitals_rdd.filter(lambda row: row != hospital_bc.value) # remove the first line (header) from our RDD
    hospitals_rdd = hospitals_rdd.mapPartitions(lambda y: map(lambda x: (x[0], create_set(x, hospital_bc)), y)).reduceByKey(lambda x, y: x.union(y))
    print("Sparse Matrix is done, now filtering...")
    global length 
    length = hospitals_rdd.count()
    ### CHECKPOINT II.A
    requested = ['150034', '050739', '330231', '241326', '070008']
    # requested = sc.parallelize(requested).map(lambda d: (d,0))
    # # print(requested.collect())
    requested_hospitals = hospitals_rdd.filter(lambda x: x[0] in requested)
    # requested_hospitals = hospitals_rdd.join(requested).map(lambda x: (x[0], x[1][0])) # filtering out the requested ones
    pp.pprint(requested_hospitals.collect())
    return hospitals_rdd

##########################################################################
##########################################################################
# TASK II.B: Minhash
# 1st task: extract features
# 2nd task: allows you to convert properties we found in task 2A into hash values
# need to use spark implementation of minhashing approach, check google and find basic implementation and adjust to our data
# 3rd task: now that we have hash values, lets use the signature (LSH) to compute the similarity between all hospitals provided in task 2C (20 most similar candidates)

# need 100 hash functions
def create_hash_functions():
    list = []
    for i in range(0, 100):
        a = random.randint(1, 99)
        b = random.randint(1, 99)
        c = 1000000
        # c = 943661897
        list.append((a, b, c))
    return list

def do_hashing(a, b, c, element):
    hashing = ((a*hash(element)) + b) % c 
    return hashing

hashes = create_hash_functions()

def generate_minhash(rdd):
    hospital_pk = rdd[0]
    shin = rdd[1]
    min_hash = math.inf
    mins = []
    for i in hashes:
        a, b, c = i
        f = lambda x: do_hashing(a, b, c, x)
        min_hash = min([f(x) for x in shin])
        mins.append(min_hash)
    return mins


def task2B(hospitals_rdd):
    print("##### TASK 2B #####")
    # print(hospitals_rdd.count())
    new_rdd = hospitals_rdd.mapPartitions(lambda y: map(lambda x: (x[0], generate_minhash(x)), y))
    print("Matrix is done, now filtering...")
    # pp.pprint(new_rdd.take(1))

    ### CHECKPOINT II.B
    requested = ['150034', '050739', '330231', '241326', '070008']
    # requested = sc.parallelize(requested).map(lambda d: (d,0))
    # print(requested.collect())

    # requested_hospitals = new_rdd.join(requested).map(lambda x: (x[0], x[1][0]))
    requested_hospitals = new_rdd.filter(lambda x: x[0] in requested) # filtering 
    pp.pprint(requested_hospitals.collect())
    return new_rdd


def lsh(sig):
    # print("sig: ")
    # pp.pprint(sig)
    bands = math.floor(length * 0.125)
    rows = 8
    hospital_pk = sig[0]
    signature = sig[1]
    a = random.randint(1, 100)
    b = random.randint(1, 100)
    c = 1000000
    # a, b, c = hashes[random.randint(1, 1600)]
    buckets = []
    
    for x in range(0,bands):
        band = x
        y = x*rows
        string = ' ' 
        column = string.join(str(i) for i in signature[y:y+rows])
        bucket = do_hashing(a, b, c, column)
        buckets.append(((band, bucket), hospital_pk))
    
    return iter(buckets)

##########################################################################
##########################################################################
# TASK II.C: Find similar pairs using LSH
# Run LSH to find approximately 20 candidates that are most similar to hospitals: 150034, 50739, 330231,241326, 70008.
# From the perspective of LSH, each hospital is a column with each row being a value of the signatures. Tweak bands and rows per band in order to get approximately 20 candidates (i.e. anything between 10 to 30 candidates per hospital is ok).  While pre existing hash code is fine, you must implement LSH otherwise.

def jaccard_similarity(sig_one, sig_two):
    sig_one = set(sig_one)  
    sig_two = set(sig_two)
    i = sig_one.intersection(sig_two)
    val = len(sig_one)+len(sig_two)-len(i)
    if val == 0: return 0
    return len(i)*1.0/val

def task2CI(hospitals_rdd):
    print("##### TASK 2C #####")
    candidates = hospitals_rdd.flatMap(lsh).groupByKey().mapPartitions(lambda y: map(lambda x: (x[0], list(x[1])), y)).flatMap(lambda x: [(item, x[1]) for item in x[1]]).reduceByKey(lambda a,b: list(set(a+b)))
    return candidates

def task2CII(sig, cand):
    requested = ['150034', '050739', '330231', '241326', '070008']
    requested_can = cand.filter(lambda x: x[0] in requested).collect()
    requested_sig = sig.filter(lambda x: x[0] in requested).collect()
    for i in range(0, 5):
        print("Hospital PK: "+ requested[i])
        cands = requested_can[i][1]
        cands_sigs = sig.filter(lambda x: x[0] in cands and x[0] != requested[i]).collect()
        count = 0
        for j in cands_sigs:
            print("\t#" + str(count+1)+ " Candidate Hospital PK: " + j[0])
            print("\tFirst 10 Values of Signature Matrix: " + str(j[1][0:10]))
            print("\tJaccard Similarity: " + str(jaccard_similarity(requested_sig[i][1], j[1])))
            print("")
            count +=1
            if count >= 20:
                break



##########################################################################
##########################################################################
# MAIN: the code below setups up the stream and calls the methods
if __name__ == "__main__":
    input_file = sys.argv[1]
    sc = SparkContext(appName="Homework 2")
    # method quieted the output logs used from https://stackoverflow.com/questions/27781187/how-to-stop-info-messages-displaying-on-spark-console
    logger = sc._jvm.org.apache.log4j
    logger.LogManager.getLogger("org"). setLevel( logger.Level.ERROR )
    logger.LogManager.getLogger("akka").setLevel( logger.Level.ERROR )
    hospitals_rdd=sc.textFile(input_file, 64)
    sparse_rdd = task2A(hospitals_rdd).cache()
    signatures = task2B(sparse_rdd).cache()
    print("")
    candidates = task2CI(signatures).cache()
    task2CII(signatures, candidates)



################ NOTES ################
# PART II.B
    # testing this
    # pp.pprint(new_rdd.take(1))
    # new_rdd = hospitals_rdd.map(lambda x: )
    # MinHash applies a random hash function g to each element in the set and take the minimum of all hashed values:
    # instead of the for loops, use maps and reduce (any of the spark transformations) also reducebykey
    # what function are we implementing
    # will it be a set of hashes like
    # (id number, set(0.23, 0.00034, 0.55, ....)) ? or one hash (id number, set(0.23824)) ?
    # (id num, [] list of 100 mins, but sid[hash min]])
    # hosp id, elements would be input (as the rdd)
    # elements are a list of strs, each of which need to be hashed 100x once for each of the 100 hash functions
    # output: for every hash func, what was the min value that any of the features hashed to
    # don't need to know which feature
    # 100 numbers that represent what's been seen thus far (signature id)
    # sig matrix
    # ((hospital pk (col), hash index (row)), min value) --> defines what's in the sig matrix
    # (('330231', 0), 3036588)
    # (('330231', 1), 2299385)
    # what is the sig matrix where is it hmmm
    # doesn't have to be whole row stored together
# PART II.C
    # Divides a signature into bands of size bandwidth
    # For rows in each band, compute a hashcode. The hashcode is
    # decimal representation of string formed by characters in a row:
    # For ex, if a row is 102a, its hashcode is 10 + 2*2 + 0*4 + 1*8 = 22
    # This hashfunction is unique for every unique key, which is very good for LSH
    # Output of this function is bucket number (i.e. hashcode) for every band in the signature.
    # Bandwidth parameter was tweaked to get 15-25 candidates for every query image
    # first we should split our signature vectors
    # Imagine we split a 100-dimensionality vector into 20 bands. That gives us 20 opportunities to identify matching sub-vectors between our vectors.
    # 20 bands of 5 rows ?

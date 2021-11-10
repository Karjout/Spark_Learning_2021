from pyspark import SparkContext, SparkConf
# What is the SparkContext
""" A SparkContext represnts the connection to Spark Cluster,
    and can be used to create RDDs,accumulators and broadcast variables on that cluster
    
"""
conf = SparkConf().setAppName("Test_RDDs")\
    .setMaster("local")
sc = SparkContext(conf=conf)
print(sc.getConf().getAll())
#sc.stop()
""" Transformation in RDD helps us to create new RDD 
    Action will return a Value

    """
#Creating RDDs
names  = sc.parallelize(['abdeslam','hanae','said','faiz','naima','ismail','soufiane','ikrame','mohamed'])
print(type(names))
print(names.collect())
print (names.countByValue())
def show(x):
    print(x)
    a=sc.parallelize([1,2,3,4,5].foreach(lambda x: print(x)))


employes = sc.textFile("emloyees.txt") 
print(type(employes))     
##Imports
import pyspark
import rtree
import geopandas as gpd
import shapely.geometry as geom
import sys

### yellow_tripdata_2011-05.csv

### 0  vendor_id,pickup_datetime,dropoff_datetime,passenger_count,
### 4  trip_distance,pickup_longitude,pickup_latitude,rate_code,
### 8  store_and_fwd_flag,dropoff_longitude,dropoff_latitude,
### 11 payment_type,fare_amount,surcharge,mta_tax,tip_amount,
### 16 tolls_amount,total_amount

import operator
import heapq

def tripMapper(records):
    import rtree
    import geopandas as gpd
    import shapely.geometry as geom
    #first create index
    index = rtree.Rtree()
    hoods = gpd.read_file('neighborhoods.geojson')
    for idx,geometry in enumerate(hoods.geometry):
        index.insert(idx,geometry.bounds)
    #for each record yield ((nei_idx,boro_idx), 1)
    neiborhood_name = "Undefined"
    boro_name = "Undefined"
    for record in records:
        list_record = record.split(',')
        if  record != '' and list_record[0] != 'vendor_id':#check for empty row and header
            
            pux = list_record[5]#pick up location x coordinate
            puy = list_record[6]#pick up location y coordinate
            dox = list_record[9]#drop off location x coordinate
            doy = list_record[10]#drop off location y coordinate

            if pux == '' or puy =='' or dox == '' or doy == '':
               continue

            point_origin      = geom.Point(float(pux),float(puy))
            point_destination = geom.Point(float(dox),float(doy))

            #get origin neighborhood
            matches = list(index.intersection((point_origin.x,point_origin.y)))
            for ind in matches:
                if any(map(lambda x: x.contains(point_origin), hoods.geometry[ind])):
                    neiborhood_name = hoods.neighborhood[ind]
            #get destination borough
            matches = list(index.intersection((point_destination.x,point_destination.y)))
            for ind in matches:
                if any(map(lambda x: x.contains(point_destination), hoods.geometry[ind])):
                    boro_name = hoods.borough[ind]
            yield ((neiborhood_name,boro_name), 1)


def top3Reducer(lst1,lst2):
    return heapq.nlargest(3,lst1+lst2,lambda x: x[1])


if __name__=='__main__':
    if len(sys.argv)<3:
        print "Usage: <input files> <output path>"
        sys.exit(-1)

    sc = pyspark.SparkContext()

    trips = sc.textFile(','.join(sys.argv[1:-1]))

    output = sc.parallelize(trips.mapPartitions(tripMapper).reduceByKey(operator.add).\
                map(lambda x: (x[0][1],[(x[0][0],x[1])])).reduceByKey(top3Reducer).collect())


    output.saveAsTextFile(sys.argv[-1])

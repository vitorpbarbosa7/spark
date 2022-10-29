from pyspark import SparkConf, SparkContext
import collections 

conf = SparkConf().setMaster("local").setAppName("RatingHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("ml-100k/u.data")
print('Lines')
print(type(lines))
print(lines)

# for each line, split it and return the third element, which is the rating itself
ratings = lines.map(lambda x: x.split()[2])
print('\nRatings')
print(type(ratings))
print(ratings)

result = ratings.countByValue()
print('\nresult')
print(type(result))
print(result)

print(f'Sorted without OrderedDict object')
for key, value in sorted(result.items()):
    print(f'{key ,value}')

print(f'With OrderedDict as the professor did')
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print(f'{key, value}')

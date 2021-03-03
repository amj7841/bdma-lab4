
from mrjob.job import MRJob
class MRStationCount(MRJob):
  def mapper(self, _, line):
    row = line.split(',')
    yield (row[10], 1)
    yield (row[6], 1)

  def reducer(self, station, counts):
    yield (station, sum(counts))

stationCount = MRStationCount(args=[]) #package to initiate the object
counts = list(mr.runJob(enumerate(open('citibike.csv', 'r')), stationCount))
counts[:10]


if __name__ == '__main__':
    MRStationCount.run()

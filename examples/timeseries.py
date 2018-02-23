""" Example of usage, high-level API. """

import haste
from   haste.hastestorageclient import HasteStorageClient

import numpy 


class HasteCollection: 
   """ Collection of tuples """

class Experiment(HasteCollection):

class TimeSeries:
    """ Encodes a time series object. """ 

    def __init__(self):

# The storage client will be used to handle data
sc = HasteStorageClient()

# Create an experiment 
# name should probably be unique, and map directly to the "root stream id"
E = haste.Experiment(storage_client=sc)

# Add a time series to the experiment
ts = haste.TimeSeries()

# By adding the ts to the experiment, it will be assigned a unique substream-id, 
# linked somehow to the root stream id of the experiment. 
E.add(ts)

# Now add (time, spatial_data_frame) tuples to the timeseries.
# They should be automatically handled by the storage client (i.e. passed through feature exraction, classification and policy-evaluation) 
tspan = numpy.linspace(0,10.0,100)
for t in tspan:
    data = 100*["Large spatial dataframe goes here."]
    ts.append((t,data))




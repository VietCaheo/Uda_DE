""" Introduce about DATALAKE concept

    15. Map Reduce: (Hadoop supports the MapReduce Technique call `Hadoop MapReduce`)
        Is a programming technique to manipulate large data sets
        concept: 
            >>First dividing up a large dataset
                >> Distributing data across a cluster
                    >> each Data is analyzed and converted into a (key, value) pair.
                        >> in Reduce step: the values with the same key are combined together.
         Spark not support MapReduce -> need to write bahaviour to same like on Hadoop
 """
# Map Reduce Exercise : Count how many times each song was played
# Install mrjob library. This package is for running MapReduce jobs with Python
# In Jupyter notebooks, "!" runs terminal commands from inside notebooks 

! pip install mrjob

%%file wordcount.py
# %%file is an Ipython magic function that saves the code cell as a file

from mrjob.job import MRJob # import the mrjob library

class MRSongCount(MRJob):
    
    # the map step: each line in the txt file is read as a key, value pair
    # in this case, each line in the txt file only contains a value but no key
    # _ means that in this case, there is no key for each line
    def mapper(self, _, song):
        # output each line as a tuple of (song_names, 1) 
        yield (song, 1)

    # the reduce step: combine all tuples with the same key
    # in this case, the key is the song name
    # then sum all the values of the tuple, which will give the total song plays
    def reducer(self, key, values):
        yield (key, sum(values))
        
if __name__ == "__main__":
    MRSongCount.run()

# run the code as a terminal command
! python wordcount.py songplays.txt

""" 17. Spark Clusters: Distributed Computing -> refer to a big computation executing, across a cluster of Nodes.

Computational framkwork: 
    Master -> Workers

Four different modes to setup Sparks:
    >> Local Mode: dont really do any distributed computing .
    >> Spark's own Standalone Customeer Manager
    >> [this course scope]: using Standalone Mode for set up Distributed Spark cluster.
    >> Spark 's Standalone Mode: there is a driver process  -> direct interracting with the driver program.
    >> YARN: from Hadoop project
    >> Open source from UC Berkely's AMPLab Coordinators.
 """

""" 18. Spark use cases: use only for big Data
        mean: the data larger than capability or a local computer memory
    -> Data Analytics
    -> Machine Learning
    -> Streaming
    -> Graph Analytics 

in case data sets not so big, (that still can be saved in local computer)
some option for manipulating data:
    >> AWK
    >> R
    >> Python libraries: Pandas, Matplotlib, Numpy, sci-kit learn among other libraries.

even, data is bigger than computer memory a bit, still can use Pandas, cause Pandas can manipulate chunk by chunk.

>> If Data is stored in a relational Data as: MySQL/ Postgres -> can use Extract, Filter and Aggregate the data.
>> Use libraries: SQLAlchemy (https://www.sqlalchemy.org/)
    provides an abstraction layer to manipulate SQL tables with generative Python expressions.
>>scikit-learn: https://scikit-learn.org/stable/
    Mostly use for Machine Learning with wide range algorithms .
    More complex algorithms as Deep Learning: TensorFlow or PyTorch.
        https://www.tensorflow.org/
        https://pytorch.org/
 
-> Spark Limitation

"""






# %%

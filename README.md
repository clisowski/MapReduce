# MapReduce
In 2004, engineers at Google introduced a new paradigm for large-scale parallel data processing known as MapReduce (see the original paper here 
(Links to an external site.), and make sure to look in the citations at the end). One key aspect of MapReduce is that it makes programming such 
tasks on large-scale clusters easy for developers; instead of worrying about how to manage parallelism, handle machine crashes, and many other 
complexities common within clusters of machines, the developer can instead just focus on writing little bits of code (described below) and the 
infrastructure handles the rest.

In this project, you'll be building a simplified version of MapReduce for just a single machine. While somewhat easier to build MapReduce for a single machine, 
there are still numerous challenges, mostly in building the correct concurrency support. Thus, you'll have to think a bit about how to build the MapReduce 
implementation, and then build it to work efficiently and correctly. For some additional practice with concurrency, you will modify an existing HashMap implementation
to be multi-threaded by adding reader-writer locks.

There are three specific objectives to this assignment:

To learn about the general nature of the MapReduce paradigm.
To implement a correct and efficient MapReduce framework using threads and related functions.
To gain more experience writing concurrent code.

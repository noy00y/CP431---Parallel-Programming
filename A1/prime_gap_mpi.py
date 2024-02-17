
from mpi4py import MPI
import time
import sys
from prime_gap import prime_finder_v2_parallel

# For input of 1,000,000,000
#mpiexec -n 4 python3 prime_gap_mpi.py 1000000000

comm = MPI.COMM_WORLD

size = comm.Get_size()
rank = comm.Get_rank()  

input_array = []
max_input = None

# First process creates the input array
if rank == 0:

    max_input = int(sys.argv[1])

    assert max_input > 2 # Make sure input at least 2

    # Split into the number of cores and put it into an array
    

    split = int(max_input/size) # Divided by # of processors

    extra = max_input % size 

    input_array = []


    for i in range(1, size+1):
        input_array.append(split * i)

    input_array[-1] += extra # if uneven give remainder to last process
    
    
# Broadcast array to all processes 
split_array = comm.bcast(input_array, root=0)
max_input = comm.bcast(max_input, root=0)

# Run Parallel Prime Gap Finder on each process' assigned range
for i in range(0, size):
    
    if rank == i: # For every rank do below:

        comm.Barrier()
        start_time = MPI.Wtime()

        if rank == 0: # First process (start at 0, go to split_array[1])
            biggest_gap, p1, p2 = prime_finder_v2_parallel(0, split_array[i], False, max_input)

        if rank == size-1: # Last process 
            biggest_gap, p1, p2 = prime_finder_v2_parallel(split_array[i-1], split_array[i], True, max_input)

        else: # All other processes
            biggest_gap, p1, p2 = prime_finder_v2_parallel(split_array[i-1], split_array[i], False, max_input)

        comm.Barrier()
        end_time = MPI.Wtime()

        # Get the result of every process
        result = comm.gather((biggest_gap, p1, p2), root=0)

        

if rank == 0:        
    biggest_gap_overall = max(result, key=lambda x: x[0])
    print(f"Biggest Gap: {biggest_gap_overall[0]} between {biggest_gap_overall[1]} and {biggest_gap_overall[2]}")
    print(f"Elapsed time: {end_time - start_time} seconds")





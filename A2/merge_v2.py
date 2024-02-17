from mpi4py import MPI
import time
import random
import math

def binary_search(array_B, num) -> int:
    low = 0
    high = len(array_B)
    
    while low < high:
        mid = (low + high) // 2
        if array_B[mid] < num:
            low = mid + 1
        else:
            high = mid
    
    return low

def generate_lists(len_A, len_B):
	# Define lists to hold values
	listA = [0]
	listB = [0]
	
	for i in range(len_A-1):
		listA.append(int(random.uniform(listA[i], listA[i]+20)))
	
	for i in range(len_B-1):
		listB.append(int(random.uniform(listB[i], listB[i]+20)))
	
	return listA, listB

def sequential_merge(array_A, array_B):
	"""
	Purpose:
	    Sequentially merge two given lists
	Inputs:
		array_A - List of integers sorted in ascending order
		array_B - List of integers sorted in ascending order
	Returns:
		merged_list - A list composed of values in array_A and
					  array_B sorted in ascending order.
	"""
	
	#Initialize variables
	i = 0
	j = 0
	merged_list = []
	
	#Calculate lengths of each array
	lenA = len(array_A)
	lenB = len(array_B)
	
	#Loop until one array is emptied
	while i < lenA and j < lenB:
		if array_A[i] <= array_B[j]:
			merged_list.append(array_A[i])
			i = i + 1
		else:
			merged_list.append(array_B[j])
			j = j + 1
	
	#Empty whichever array hasn't been emptied yet
	if i == lenA:
		merged_list = merged_list + array_B[j:]
	else:
		merged_list = merged_list + array_A[i:]
	
	return merged_list

def diagnostic_function(listA, listB, merged_list, quiet):
	"""
	Purpose:
		Display diagnostic data about the provided lists
	Inputs:
		listA - A sorted array of integer values
		listB - A sorted array of integer values
		merged_list - A sorted array composed of listA and listB
		quiet - Boolean value to indicate whether the function should
				display the given lists or not. For higher lengths 
				of lists using quiet is advised.
	Warning:
		This function runs in serial. Running with approximately 
		~100000000 values in each list causes function to run 
		extremely slowly.
	"""
	
	lenA = len(listA)
	lenB = len(listB)
	len_merged = len(merged_list)
	missing_vals = []
	
	#Output the list lengths and arrays
	print("List Lengths: " + str(lenA) + " " + str(lenB) + " " + str(len_merged))
	
	if (not quiet):
		print("\nArrays Values:\n")
		print("List A: " + str(listA))
		print()
		print("List B: " + str(listB))
		print()
		print("List M: " + str(merged_list))
		print()
	
	#Check for missing values in the merged array
	for x in listA:
		if (x not in merged_list):
			missing_vals.append(x)
	
	for x in listB:
		if (x not in merged_list):
			missing_vals.append(x)
	
	print("Missing Values: " + str(missing_vals))
	
	return
			
def parallel_merge(length_A, length_B) :
    """
	Purpose:
        Use MPI to merge two sorted lists into one
	Inputs:
        length_A - The length of list to generate
		length_B - The length of list to generate
	"""

    # MPI Init
    comm = MPI.COMM_WORLD

    size = comm.Get_size()
    rank = comm.Get_rank()  


    # Init vars
    array_A = None
    array_B = None
    chunks_A = []
    chunks_B = []
    
    # First process creates the arrays and determines the chunks
    if rank == 0:
        #Start counting time
        start_time = MPI.Wtime()

        array_A, array_B = generate_lists(length_A, length_B)

        #Define needed variables
        start_index = 0
        end_index = 0
        b_start = 0
        index = 0

        #Generate chunks of array_A to be sent to each process
        for i in range(size):
            #Determine the chunk of A to send to process with rank i
            if (rank < len(array_A)%size):
                end_index = start_index + math.floor((len(array_A) / size)) + 1
            else:
                end_index = start_index + math.floor((len(array_A) / size))
            chunks_A.append(array_A[start_index:end_index])
            start_index = end_index
        	
        #Determine the chunk of B to send to process with rank i
        for chunk in chunks_A:
            index = binary_search(array_B, chunk[-1])
            chunks_B.append(array_B[b_start:index])
            b_start = index
        
        if index != len(array_B):
            chunks_B[-1] = chunks_B[-1] + array_B[index:]
        
        #Determine amount of time to setup data
        setup_time = MPI.Wtime()
        
    #Share chunks to other processes
    chunks_A = comm.scatter(chunks_A, root=0)
    chunks_B = comm.scatter(chunks_B, root=0)
    
    #Wait for all processes to start merging at the same time
    comm.barrier()
    
    #Generate the merged array
    merged_array = sequential_merge(chunks_A, chunks_B)
    
    result = comm.gather((merged_array, rank), root=0) #Gather the completed list of values
    
    #Wait for all processes to finish sorting their array
    comm.barrier()
    
    if rank == 0:
        final_sorted_array = []
        array_process_tuple = result
        
        for i in range(0,size):
            for tuple in array_process_tuple:
                if tuple[1] == i:
                    final_sorted_array = final_sorted_array[0]
        
        end_time = MPI.Wtime()
        print("Time Total: " + str(end_time - start_time))
        print("Time without setup: " + str(end_time - setup_time))
        MPI.Finalize()
        return (array_A, array_B, final_sorted_array)
    
    #Clean up MPI
    MPI.Finalize()
    return

































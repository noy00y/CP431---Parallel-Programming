import merge_v2

#Set diagnostic to True to display information about given and produced arrays
diagnostic = True

#Adjust lengths of lists here
length_A = 5000
length_B = 5000

arrays = merge_v2.parallel_merge(length_A, length_B)


if arrays != None and diagnostic:
    merge_v2.diagnostic_function(arrays[0], arrays[1], arrays[2], True)
import gmpy2
import math

# Find primes serially
def prime_finder_v2_serial(upper):
    prime = gmpy2.mpz(2)
    prev_prime = None
    biggest_gap = 0
    p1 = None
    p2 = None
    while prime < upper:
        prev_prime = prime
        prime = gmpy2.next_prime(prime)
        gap = prime - prev_prime
        if gap > biggest_gap:
            biggest_gap = gap
            p1 = prev_prime
            p2 = prime
    return biggest_gap, p1, p2

# Find primes parallely
def prime_finder_v2_parallel(start, upper_limit, is_last_process, max_input):
    prime = gmpy2.next_prime(start - 1) # Get the first prime from start var
    prev_prime = None
    biggest_gap = 0
    p1 = None
    p2 = None
    
    # If it's the last process we don't need to deal with the edge case of a large gap being in-between processes
    if is_last_process:

        while prime < upper_limit: 
            prev_prime = prime
            prime = gmpy2.next_prime(prime) # gets next prime

            if prime > upper_limit: # Make sure the prime number doesnt go above the upper limit
                prime = upper_limit

            gap = prime - prev_prime 
            
            
            if gap > biggest_gap: # Finds biggest gap
                biggest_gap = gap
                p1 = prev_prime
                p2 = prime
        return biggest_gap, p1, p2
    
    # This deals edge case of a large gap being in-between processes by the process extending its search slightly beyond its initial range until it finds another prime
    else:


        while prime < max_input and (prime < upper_limit) or (prev_prime != None and prev_prime < upper_limit):
            
            prev_prime = prime
            prime = gmpy2.next_prime(prime)

            if prime > max_input: # Cannot be greater than max input
                prime = upper_limit

            gap = prime - prev_prime
            if gap > biggest_gap:
                biggest_gap = gap
                p1 = prev_prime
                p2 = prime

        return biggest_gap, p1, p2
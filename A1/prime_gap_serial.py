from prime_gap import prime_finder_v2_serial
import time

start = time.time()
gap, p1, p2 = prime_finder_v2_serial(900000000)
end = time.time()
time_elapsed = end - start
print("Running serially")
print("first prime: {}, second prime: {}, gap: {}, time elapsed: {}".format(p1, p2, gap, time_elapsed))
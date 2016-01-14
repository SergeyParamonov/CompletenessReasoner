from multiprocessing import Pool, cpu_count
import time
import pickle
import sys
from solver import done_keyword

def main():
  process_blob(sys.argv[1])

def process_chunk(chunk):
  return chunk.process_chunk()

def load_chunks(filename):
    with open(filename, "rb") as inputfile:
      return pickle.load(inputfile)
   
def process_blob(blob_id):
    t0 = time.time()
    blob  = load_chunks("tmp/blobs/blob{}".format(blob_id))
    cores = cpu_count()
    pool  = Pool(cores)
    results = pool.map_async(process_chunk, blob).get()
    pool.close()
    is_complete = True
    with open("sync/worker_results{}".format(blob_id),"w") as sync:
      for i,output in enumerate(results):
        q_a, inferred = output
        if not q_a:
          print("not complete for the case: "+str(i), file=sync)
          is_complete = False
      t1 = time.time()
      total_time = t1-t0
      print("runtime: "+str(total_time), file=sync)
      if is_complete:
        print("COMPLETE",file=sync)
      else:
        print("NOT_COMPLETE", file=sync)
      print(done_keyword,file=sync)



if __name__ == "__main__":
  main()

      







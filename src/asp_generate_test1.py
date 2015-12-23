from itertools import product
import os
from random import shuffle

def main():
  generate_test1()

def make_query(outputfile):
# q_i = "pupil(n, c, s). class(c, s, 1, b). school(s, sc, t). schoolcluster(sc, d, pub)."
  q_i = "pupil(n, c, s)." # test
  print(q_i, file=outputfile)

def make_TCs(C,S,outputfile):
  print("pupil_a(N,C,S) :- pupil(N,C,S).",file=outputfile)
  for c in range(C):
    for s in range(S):
      print("pupil_a(N,c{c},s{s}) :- pupil(N,c{c},s{s}).".format(c=c,s=s),file=outputfile)
  

def generate_test1(C=100, S=100):
  query = "../experiments/test1/query"
  tcs = "../experiments/test1/tcs"
  with open(query, "w") as outputfile:
    make_query(outputfile)
  with open(tcs, "w") as outputfile:
    make_TCs(C,S,outputfile)




if __name__ == "__main__":
  main()

import os
from random import shuffle

def main():
  generate_test2()

def make_query(outputfile):
  q_i = "q(N) :- pupil(N, C, S); class(C, S, 1, B); school(S, SC, T); schoolcluster(SC, D, pub)."
# q_i = "pupil(n, c, s)." # test
  print(q_i, file=outputfile)

def make_TCs(C,S,outputfile):
  print("pupil_a(N,C,S) :- pupil(N,C,S).",file=outputfile)
  print("class_a(C, S, 1, B) :- class(C, S, 1, B).",file=outputfile)
  print("school_a(S, SC, T) :- school(S, SC, T).",file=outputfile)
  print("schoolcluster_a(SC, D, pub) :- schoolcluster(SC, D, pub).",file=outputfile)
# for c in range(C):
#   for s in range(S):
#     print("pupil_a(N,c{c},s{s}) :- pupil(N,c{c},s{s}).".format(c=c,s=s),file=outputfile)


def make_CFDCS(C,S,outputfile):
  cVals = "{" + ",".join(map(lambda c:"c_"+str(c),range(C))) + "}"
  print("class;1;{cVals};-;-".format(cVals=cVals),file=outputfile)
  sVals = "{" + ",".join(map(lambda s:"s_"+str(s),range(S))) + "}"
  print("class;2;{sVals};-;-".format(sVals=sVals),file=outputfile)

  

def generate_test2(C=100, S=100):
  query = "../experiments/test2/query"
  tcs   = "../experiments/test2/tcs"
  cfdcs = "../experiments/test2/cfdcs"
  with open(query, "w") as outputfile:
    make_query(outputfile)
  with open(tcs, "w") as outputfile:
    make_TCs(C,S,outputfile)
  with open(cfdcs, "w") as outputfile:
    make_CFDCS(C,S,outputfile)




if __name__ == "__main__":
  main()

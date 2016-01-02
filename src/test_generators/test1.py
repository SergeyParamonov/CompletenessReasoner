import os
from random import shuffle

def main():
  generate_test1()

def make_query(outputfile):
  q_i = "q(N) :- pupil(N, C, S); class(C, S, 1, B); school(S, SC, T); schoolcluster(SC, D, pub)."
# q_i = "pupil(n, c, s)." # test
  print(q_i, file=outputfile)

def make_TCs(C,S,outputfile):
  print("pupil_a(N,C,S) :- pupil(N,C,S).",file=outputfile)
  for c in range(C):
    for s in range(S):
      print("pupil_a(N,c{c},s{s}) :- pupil(N,c{c},s{s}).".format(c=c,s=s),file=outputfile)

def make_FK(outputfile):
  print("pupil(N,C,S);class(C,S,Y,B);[2,3];[1,2]",file=outputfile)
  print("class(C,S,Y,B);school(S,SC,T);[2];[1]",file=outputfile)
  print("school(S,SC,T);schoolcluster(SC,D,P);[2];[1]",file=outputfile)

  

def generate_test1(C=100, S=100):
  query = "../experiments/test1/query"
  tcs = "../experiments/test1/tcs"
  fk  = "../experiments/test1/fks"
  with open(query, "w") as outputfile:
    make_query(outputfile)
  with open(tcs, "w") as outputfile:
    make_TCs(C,S,outputfile)
  with open(fk, "w") as outputfile:
    make_FK(outputfile)




if __name__ == "__main__":
  main()

# datalog classes 
from atom import Atom
from rule import Rule
from parser import Parser
from grounder import Grounder
from solver import CompletenessSolver
# test generator
from test_generators.test1 import generate_test1
from test_generators.test2 import generate_test2

from collections import defaultdict
import operator
import sys
import time
from multiprocessing import Pool
from itertools import product
import math

                                           
def run_test1():
  t0 = time.time()
  experiment_folder = "../experiments/test1/"
  query_file = experiment_folder+"query"
  tcs_file   = experiment_folder+"tcs"
  fk_file    = experiment_folder+"fks"
# cfds_file  = experiment_folder+"cfdcs"
  solver = CompletenessSolver(query_file, tcs_file, fk_file, fk_semantics=True, query_semantics="bag")
  q_a, inferred = solver.check_query()
  t1 = time.time()
  total_n = t1-t0
  print(solver.q_a)
  print('query results ',q_a)
  print("inferred:",inferred)
  print("Total seconds {}".format(str(total_n)))
  print("Grounding set")
  print(solver.grounder.grounding_set)

def run_test2():
  p = Parser()
  experiment_folder = "../experiments/test2/"
  fdc_file = experiment_folder+"cfdcs"
  query_file = experiment_folder+"query"
  tcs_file   = experiment_folder+"tcs"

  t0 = time.time()
  solver = CompletenessSolver(query_file, tcs_file, fk_file=None, fk_semantics=None, cfdc_file=fdc_file, query_semantics="bag")
  q_a = solver.check_query()
  t1 = time.time()
  total_n = t1-t0

  print(solver.q_a)
  print('query results ',q_a)
# print("inferred:",inferred)
  print("Total seconds {}".format(str(total_n)))
  print("Grounding set")
  print(solver.grounder.grounding_set)
  print(solver.number_of_cases)
  print(solver.cases_vars)


def main():
  C = 10
  S = 10
  print('test 2 generating C={C}, S={S}'.format(C=C,S=S))
  generate_test2(C,S)
  print('running')
  run_test2()

# C=1000
# S=1000
# print('generating C={C}, S={S}'.format(C=C,S=S))
# generate_test1(C,S)
# print('running')
# run_test1()
# p = Parser()
# atom1 = p.parse_atom("class(c,s,f_3[c;s],f_4[c;s])")
# atom2 = p.parse_atom(" class(c,s,1,b)")
# print(atom1)
# print(atom2)
# print(CompletenessSolver.equal_upto_functional_terms(atom1,atom2))
# p = Parser()
# r_i,r_a = p.parse_FK("pupil(N,C,S); class(C,S,X1,X2);   [ 2 ,  3]; [   1  ,2]",enforced_semantics=True)
# r = p.parse_rule("class(C,S,f_1[C;S],f_2[C;S]) :- pupil(N,C,S).")
# gs = p.parse_atoms("pupil(n1,c1,s1). pupil(n2,c2,s2). class(c1,s1,cnst1,const2).")
# g = Grounder(gs)
# ground_rules = g.ground_rule(r)
# 
# for rule in ground_rules:
#   print(rule)
#   answer = CompletenessSolver.infer_rule(rule,gs)
#   print(answer)

if __name__ == "__main__":
  main()

# datalog classes 
from atom import Atom
from rule import Rule
from parser import Parser
from grounder import Grounder
# test generator
from test_generators.test1 import generate_test1

from collections import defaultdict
import operator
import sys
import time
from multiprocessing import Pool
from itertools import product
import math


class CompletenessSolver():

  num_of_cores_to_use = 8
  
  @staticmethod
  def infer_rule(rule,grounding_set):
    head, body = rule.get_tuple()
    body_set   = set(body)
    facts      = set(grounding_set)
    if body_set.issubset(facts):
      return head
    else:
      return None

  def update_grounding_set(self, fks_i):
    inferred = set()
    grounding_set = self.grounder.grounding_set[:]
    for rule in fks_i:
      grounded_rules = self.grounder.ground_rule(rule)
      inferred       = inferred.union(self.infer(grounded_rules))
    grounding_set = list(inferred.union(set(grounding_set)))
    grounding_set = self.clean_after_FK(grounding_set)
    self.grounder = Grounder(grounding_set)

  @staticmethod
  def clone_ideal_to_available_for_grounding(ideal_ground_atoms):
    atoms = []
    for atom in ideal_ground_atoms:
      p, args = atom.get_tuple()
      atoms.append(Atom(p+"_a",args))
    return atoms


  def apply_fks_a(self, fks_a, inferred_available):
    new_grounding_set = set(inferred_available) | set(self.grounder.grounding_set)
    grouding_ideal_2_avail_atoms = set(self.clone_ideal_to_available_for_grounding(self.grounder.grounding_set)) # we need to ground available atoms in FK_a using constants from ideal atoms too
    grounder          = Grounder(new_grounding_set | grouding_ideal_2_avail_atoms) 
    grounded_rules    = grounder.ground_rules(fks_a)
    print("NEW GROUNDIN SET", new_grounding_set)
    print("FKS_GROUNDED",grounded_rules)
    old_inferred = new_grounding_set.copy()
    while True:
      new_inferred = self.infer(grounded_rules,grounding_set=old_inferred)
      inferred = set(new_inferred) | old_inferred
      if inferred == old_inferred:
        print("NEW INFERRED", new_inferred)
        all_available = new_inferred | set(inferred_available)
        return all_available
      else:
        old_inferred = inferred.copy()

  def check_query(self):
    self.read_query(self.query_file)
    fks_a = [] # default empty rules for fk_a
    if self.fk_file and self.fk_semantics:
      fks_i, fks_a = self.read_FKs()
      self.update_grounding_set(fks_i)
    inferred_available = self.infer_TCs(self.tcs_file)
    inferred_available = self.apply_fks_a(fks_a, inferred_available)
    q_a_grounder   = Grounder(inferred_available)
    grounded_rules = q_a_grounder.ground_rule(self.q_a)
    q_a            = self.infer(grounded_rules,grounding_set=inferred_available)
    return q_a, inferred_available
    
  
  def infer(self, ground_rules,grounding_set=None):
    if grounding_set is None:
      grounding_set = self.grounder.grounding_set
    inferred_facts = set()
    for rule in ground_rules:
      inferred_fact = self.infer_rule(rule,grounding_set)
      if inferred_fact:
        inferred_facts.add(inferred_fact)
    return inferred_facts

  def create_q_a(self,head,body):
    q_a_body = []
    head_a = self.parser.parse_atom(head)
    p, args = head_a.get_tuple()
    q_a_head = Atom(p+"_a",args)
    body = body.strip(" .")
    body_atoms = body.split(";")
    for atom_str in body_atoms:
      atom = self.parser.parse_atom(atom_str)
      p, args = atom.get_tuple()
      available_atom = Atom(p+"_a",args) # bag semantics, everythings is already assumed to be grounded in the q_a
      q_a_body.append(available_atom)
    q_a = Rule(q_a_head,q_a_body)     
    return q_a

  def read_query(self,filename):
    with open(filename, "r") as q_file:
      query_str     = q_file.read().strip(" \n\r\t")
      head, body    = query_str.split(":-")
      grounding_set = self.parser.parse_atoms(body)
      self.grounder = Grounder(grounding_set)
      self.q_a      = self.create_q_a(head.strip(),body)
   
   
  def __init__(self, query_file, tcs_file, fk_file=None, fk_semantics=None, query_semantics="bag"):
    self.query_file = query_file
    self.tcs_file = tcs_file
    self.fk_file  = fk_file
    self.fk_semantics = fk_semantics
    self.query_semantics = query_semantics
    self.parser = Parser()

  
  def read_FKs(self):
    fks_i = []
    fks_a = []
    with open(self.fk_file,"r") as fk_file:
      fks_str = fk_file.read().splitlines()
      for fk_str in fks_str:
        fk_i,fk_a = self.parser.parse_FK(fk_str, self.fk_semantics)
        if fk_a:
          fks_a.append(fk_a)
        fks_i.append(fk_i)
    return fks_i, fks_a


  def process_rules(self,data):
    inferred = set()
    for indx, line in enumerate(data):
      rule = self.parser.parse_rule(line)
      grounded_rules = self.grounder.ground_rule(rule)
      inferred = inferred.union(self.infer(grounded_rules))
    return inferred

  def split_tcs(self,data,k):
    size = len(data) 
    chunk_size = int(math.ceil(size/float(k)))
    chunks = []
    for i in range(k):
      left_bound = i*chunk_size
      if (i+1)*chunk_size >= size:
        right_bound = size
      else:
        right_bound = (i+1)*chunk_size
      chunks.append(data[left_bound:right_bound])
    return chunks


  def infer_TCs(self,filename):
    with open(filename, "r") as tc_file:
      data = tc_file.read().splitlines()
      k = self.num_of_cores_to_use
      pool = Pool(k)
      data_list = self.split_tcs(data,k)
      inferred_list = pool.map(self.process_rules, data_list)
      inferred = set()
      for fact_set in inferred_list:
        inferred = inferred.union(fact_set)
      return inferred

  @staticmethod
  def equal_upto_functional_terms(atom1, atom2):
    p1, args1 = atom1.get_tuple()
    p2, args2 = atom2.get_tuple()
    if p1 != p2:
      return False
    for term1, term2 in zip(args1,args2):  
      if (term1 != term2) and not (Parser.is_functional_term(term1) or Parser.is_functional_term(term2)):
        return False
    return True
  
                                             
  def clean_after_FK(self,grounding_set):                
    grounding_set     = grounding_set[:]
    new_grounding_set = grounding_set[:]
    for atom1, atom2 in product(grounding_set, grounding_set):
      if atom1 > atom2 and self.equal_upto_functional_terms(atom1,atom2):
        if Parser.is_functional_atom(atom1):
          new_grounding_set.remove(atom1)
        else:
          new_grounding_set.remove(atom2)
    return new_grounding_set
                                           
                                           
def run_test1():
  t0 = time.time()
  experiment_folder = "../experiments/test1/"
  query_file = experiment_folder+"query"
  tcs_file   = experiment_folder+"tcs"
  fk_file    = experiment_folder+"fks"
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


def main():
  C=1000
  S=1000
  print('generating C={C}, S={S}'.format(C=C,S=S))
  generate_test1(C,S)
  print('running')
  run_test1()
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

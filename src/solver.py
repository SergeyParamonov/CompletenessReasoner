# datalog classes 
from atom import Atom
from rule import Rule
from parser import Parser
from grounder import Grounder
from chunk import Chunk
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


class CompletenessSolver():

  num_of_cores_to_use = 8
  min_number_of_tcs_per_chunk = 10000 #the case is not split, if there fewer TCs than this limit, determinted experimentally in the pre-testing
  
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
    print("NEW GROUNDING SET", new_grounding_set)
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

  def create_cases(self):
    query_set = self.grounder.grounding_set
    fdcs      = self.cfdcs
    var2fdc   = defaultdict(list)
    number_of_cases = 1
    for fdc in fdcs:
      affected_vars = self.get_affected_vars(fdc, query_set)
      for var in affected_vars: 
        var2fdc[var].append(fdc)
    cases = defaultdict(set)
    for key, fdcs in var2fdc.items():
      values = None
      for fdc in fdcs:
        R, i, vals = fdc.get_fdc_tuple()
        if values is None:
          values = set(vals)
        else:
          values &= set(vals)
      number_of_cases *= len(values)
      cases[key] = values
    return cases, number_of_cases


  @staticmethod
  def get_affected_vars(fdc, query_set):
    affected = set()
    R, i, vasl = fdc.get_fdc_tuple()
    for atom in query_set:
      p, args = atom.get_tuple()
      if R == p:
        affected.add(args[i-1])
    return affected

  def check_query_without_cases(self):
    fks_a = [] # default empty rules for fk_a
    is_fk_present = self.fk_file and self.fk_semantics
    # infer new ideal atoms using ideal fks rules (non-enforced part)
    if is_fk_present:
      fks_i, fks_a = self.read_FKs()
      self.update_grounding_set(fks_i)
    
    # infer new available facts using TC, runs in || on a single machine
    inferred_available = self.infer_TCs(self.tcs_file)

    # apply enforced fk rules and get new available atoms
    if is_fk_present and self.fk_semantics == "enforced":
      inferred_available = self.apply_fks_a(fks_a, inferred_available)

    # set grounder to use only available atoms for grounding the query
    q_a_grounder       = Grounder(inferred_available)
     
    # ground and evaluate the query
    grounded_rules     = q_a_grounder.ground_rule(self.q_a)
    q_a                = self.infer(grounded_rules,grounding_set=inferred_available)
    return q_a, inferred_available
     

  def check_query(self):
    self.read_query(self.query_file)
    if self.cfdc_file is not None:
      self.cfdcs = Parser.parse_CFDCs(self.cfdc_file)
    else:
      return check_query_without_cases()
    
    

    is_fk_present = self.fk_file and self.fk_semantics
    fks_a = [] # default empty rules for fk_a
    if is_fk_present: 
      fks_i, fks_a = self.read_FKs()
      self.update_grounding_set(fks_i)

    #here we split cases and put them together for processing
    self.cases, self.number_of_cases = self.create_cases()
    self.cases_iter = product(*[self.cases[k] for k in self.cases.keys()]) 
    self.cases_vars = list(self.cases.keys())


    tcs = Parser.read_tcs(self. tcs_file)
    search_space_size = len(tcs) * self.number_of_cases
    chunk_size       = search_space_size/self.num_of_cores_to_use
    print("TCS len", len(tcs))
    print("search space", search_space_size)
    print("chunk size", chunk_size)

    chunks = self.split_into_chunks(self.cases_iter, self.cases_vars, tcs)
   

    inferred_available = self.infer_TCs(self.tcs_file)

    # apply enforced fk rules and get new available atoms
    if is_fk_present and self.fk_semantics == "enforced":
      inferred_available = self.apply_fks_a(fks_a, inferred_available)

    q_a_grounder       = Grounder(inferred_available)
    grounded_rules     = q_a_grounder.ground_rule(self.q_a)
    q_a                = self.infer(grounded_rules,grounding_set=inferred_available)
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
    head_a = Parser.parse_atom(head)
    p, args = head_a.get_tuple()
    q_a_head = Atom(p+"_a",args)
    body = body.strip(" .")
    body_atoms = body.split(";")
    for atom_str in body_atoms:
      atom = Parser.parse_atom(atom_str)
      p, args = atom.get_tuple()
      available_atom = Atom(p+"_a",args) # bag semantics, everythings is already assumed to be grounded in the q_a
      q_a_body.append(available_atom)
    q_a = Rule(q_a_head,q_a_body)     
    return q_a

  def read_query(self,filename):
    with open(filename, "r") as q_file:
      query_str     = q_file.read().strip(" \n\r\t")
      head, body    = query_str.split(":-")
      grounding_set = Parser.parse_atoms(body)
      self.grounder = Grounder(grounding_set)
      self.q_a      = self.create_q_a(head.strip(),body)
   
   
  def __init__(self, query_file, tcs_file, cfdc_file=None, fk_file=None, fk_semantics=None, query_semantics="bag"):
    self.query_file = query_file
    self.tcs_file = tcs_file
    self.fk_file  = fk_file
    self.fk_semantics = fk_semantics
    self.query_semantics = query_semantics
    self.cfdc_file = cfdc_file

  def process_chunk(self, chunk):
    inferred_list = self.process_rules(chunk.tcs, case_sub=chunk.substitution)
    inferred = set()
    for fact_set in inferred_list:
      inferred = inferred.union(fact_set)
    return inferred



  def split_into_chunks(self, cases_iter, case_vars, tcs):
    for case in cases_iter:
      yield Chunk(case, case_vars, tcs)
          

      

  
  def read_FKs(self):
    fks_i = []
    fks_a = []
    with open(self.fk_file,"r") as fk_file:
      fks_str = fk_file.read().splitlines()
      for fk_str in fks_str:
        fk_i,fk_a = Parser.parse_FK(fk_str, self.fk_semantics)
        if fk_a:
          fks_a.append(fk_a)
        fks_i.append(fk_i)
    return fks_i, fks_a


  def process_rules(self, data, case_sub=None):
    inferred = set()
    for indx, line in enumerate(data):
      rule = Parser.parse_rule(line)
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
    data = Parser.read_tcs(filename)
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
                                           


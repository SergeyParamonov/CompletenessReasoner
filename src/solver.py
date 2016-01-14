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
import pickle
import paramiko
import os

done_keyword = "DONE+"

class CompletenessSolver():
  blob_folder = "tmp/blobs/blob"
  results_folder = "sync/worker_results"
  
  def distribute(self,number_of_blobs, results_folder):
    username = os.environ["SSH_USERNAME"]
    password = os.environ["SSS_PASSWORD"]
    servers  = ["pinac3{}.cs.kuleuven.be".format(i) for i in range(1,number_of_blobs+1)]

    for blob_id, server in enumerate(servers):
      ssh = paramiko.SSHClient()
      ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
      ssh.connect(server, username=username, password=password)
      cmd_to_execute = "cd CompletenessReasoner/src; python3 worker.py {blob_id}; exit".format(blob_id=blob_id)
      ssh_stdin, ssh_stdout, ssh_stderr = ssh.exec_command(cmd_to_execute)
   
    self.create_sync_files(results_folder, number_of_blobs)

  def create_sync_files(self,results_folder, number_of_blobs):
    for blob in range(number_of_blobs):
      with open(results_folder+str(blob),"w") as create_file:
        pass


  @staticmethod
  def infer_rule(rule,grounding_set,case_sub=None):
    head, body = rule.get_tuple()
    body_set   = set([atom.get_case_substituted(case_sub) for atom in body])
    facts      = set([atom.get_case_substituted(case_sub) for atom in grounding_set])
    if body_set.issubset(facts):
      return head.get_case_substituted(case_sub)
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


  def apply_fks_a(self, fks_a, inferred_available, case_sub=None):
    if case_sub is not None:
      cased_inferred      = [atom.get_case_substituted(case_sub) for atom in inferred_available]
      cased_grounding_set = [atom.get_case_substituted(case_sub) for atom in self.grounder.grounding_set]
    else:
      cased_inferred      = inferred_available
      cased_grounding_set = self.grounder.grounding_set
    new_grounding_set = set(cased_inferred) | set(cased_grounding_set)
    grouding_ideal_2_avail_atoms = set(self.clone_ideal_to_available_for_grounding(cased_grounding_set)) # we need to ground available atoms in FK_a using constants from ideal atoms too
    grounder          = Grounder(new_grounding_set | grouding_ideal_2_avail_atoms) 
    grounded_rules    = grounder.ground_rules(fks_a)
  # print("NEW GROUNDING SET", new_grounding_set)
  # print("FKS_GROUNDED",grounded_rules)
    old_inferred = new_grounding_set.copy()
    while True:
      new_inferred = self.infer(grounded_rules,grounding_set=old_inferred)
      inferred = set(new_inferred) | old_inferred
      if inferred == old_inferred:
  #     print("NEW INFERRED", new_inferred)
        all_available = new_inferred | set(cased_inferred)
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
    return q_a
     

  def check_query(self):
    self.read_query(self.query_file)
    if self.cfdc_file is not None:
      self.cfdcs = Parser.parse_CFDCs(self.cfdc_file)
    else:
      return check_query_without_cases()
    

    self.is_fk_present = self.fk_file and self.fk_semantics
    fks_a = [] # default empty rules for fk_a
    if self.is_fk_present: 
      fks_i, fks_a = self.read_FKs()
      self.update_grounding_set(fks_i)

    #here we split cases and put them together for processing
    cases, number_of_cases = self.create_cases()
    cases_iter = product(*[cases[k] for k in cases.keys()]) 
    cases_vars = list(cases.keys())


    tcs = Parser.read_tcs(self.tcs_file)
    search_space_size = len(tcs) * number_of_cases
    chunk_size       = search_space_size/self.number_of_cores
 #  print("TCS len", len(tcs))
    print("search space", search_space_size)
 #  print("chunk size", chunk_size)
    
    if self.fk_semantics != "enforced":
      fks_a = None
    chunks = list(self.split_into_chunks(cases_iter, cases_vars, tcs, self.grounder.grounding_set, self, fks_a))
    number_of_blobs = 5
    blobs  = Parser.split_into_k(chunks,number_of_blobs)

    print("time before saving chunks:", "{0:.2f}".format(time.time()-self.solver_creation_time))
    for i, blob in enumerate(blobs):
        self.save_chunks(blob, self.blob_folder+"{}".format(str(i)))

    print("time before distr:", "{0:.2f}".format(time.time()-self.solver_creation_time))
    self.distribute(number_of_blobs, self.results_folder)
    print("distributed tasks, waiting for results...")
    print("time after distr:", "{0:.2f}".format(time.time()-self.solver_creation_time))
    is_complete, errors = CompletenessSolver.merge_results(number_of_blobs, self.results_folder)
   
    return is_complete 
  
  @staticmethod
  def merge_results(number_of_blobs, results_folder):
    CHECK_EXECUTION_ONCE_IN_SECONDS = 3
    all_done = False
    overall_complete = True
    errors = [] # not complete blobs
    while not all_done:
      time.sleep(CHECK_EXECUTION_ONCE_IN_SECONDS)
      all_done = True
      for blob in range(number_of_blobs):
        all_done, is_complete = CompletenessSolver.check_done_blob(blob, results_folder)
        if all_done and not is_complete:
          overall_complete = False
          errors.append(blob)

    print(errors)
    if not errors:
      errors = None
    return overall_complete, errors
    
  @staticmethod
  def check_done_blob(blob, results_folder):
    filename = results_folder+str(blob)
    with open(filename, "r") as results:
      lines = results.read().splitlines()
      if len(lines) < 3:
        for line in lines:
          print(line)
        return False, None
      if lines[-1] == done_keyword:
        if lines[-2] == "COMPLETE":
          is_complete = True
        else:
          is_complete = False
        return True, is_complete
      else:
        return False, None

  # for i, blob in enumerate(blobs):
  #   print("processing {}-th blob".format(i))
  #   self.process_blob(self.number_of_cores,blob)


  # pool = Pool(self.number_of_cores)
  # processed = pool.map_async(self.process_chunk, chunks)
    is_complete = True
  # for i, output in enumerate(processed):
  #   q_a, inferred_available, chunk = output
  #   if not q_a:
  #     print(chunk," is not complete")
  #     is_complete = False

    # print(i, q_a, inferred_available)

#   inferred_available = self.infer_TCs(self.tcs_file)

    # apply enforced fk rules and get new available atoms

  # this is only defined so we can pickle and use process_chunk of a chunk
  # in || exectution
   
  @classmethod
  def process_chunk(cls,chunk):
    return chunk.process_chunk()
   
  def process_blob(self, cores, blob):
    pool = Pool(cores)
    results = pool.map_async(CompletenessSolver.process_chunk, blob).get()
    pool.close()
    for i,output in enumerate(results):
      q_a, inferred = output
      if not q_a:
        print("not complete for the case: "+str(i))


  
  def save_chunks(self,list_of_chunks, filename):
    with open(filename, "wb") as outputfile:
      pickle.dump(list_of_chunks, outputfile)
    
  
  def load_chunks(self, filename):
    with open(filename, "rb") as inputfile:
      return pickle.load(inputfile)
        
  
  def infer(self, ground_rules,grounding_set=None, case_sub=None):
    if grounding_set is None:
      grounding_set = self.grounder.grounding_set
    inferred_facts = set()
    for rule in ground_rules:
      inferred_fact = self.infer_rule(rule, grounding_set, case_sub=case_sub)
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
   
   
  def __init__(self, query_file, tcs_file, cfdc_file=None, fk_file=None, fk_semantics=None, query_semantics="bag", number_of_cores=None):
    self.solver_creation_time = time.time()
    os.system("rm " + self.blob_folder + "*")
    os.system("rm " + self.results_folder + "*")
    self.query_file = query_file
    self.tcs_file = tcs_file
    self.fk_file  = fk_file
    self.fk_semantics = fk_semantics
    self.query_semantics = query_semantics
    self.cfdc_file = cfdc_file
    if number_of_cores:
      self.number_of_cores = number_of_cores
    else:
      self.number_of_cores = Parser.num_of_cores_to_use

  def split_into_chunks(self, cases_iter, case_vars, tcs, grounding_set, solver, fks_a):
    grounder = Grounder(grounding_set)
    for case in cases_iter:
      yield Chunk(case, case_vars, tcs, grounder, solver, fks_a)


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


  def process_rules(self, rules, grounder=None, case_sub=None):
    inferred = set()
    if grounder is None:
      grounder = self.grounder
    else:
      grounder = grounder
    for rule in rules:
      grounded_rules = grounder.ground_rule(rule)
      inferred = inferred.union(self.infer(grounded_rules, grounding_set=grounder.grounding_set,case_sub=case_sub))
    return inferred


  def infer_TCs(self,filename):
    data = Parser.read_tcs(filename)
    k    = self.number_of_cores
    pool = Pool(k)
    data_list = Parser.split_into_k(data,k)
    inferred_list = pool.map_async(self.process_rules, data_list)
    inferred      = set()
    for fact_set in inferred_list:
      inferred = inferred.union(fact_set)
    return inferred

    
                                             
  def clean_after_FK(self,grounding_set):                
    grounding_set     = grounding_set[:]
    new_grounding_set = grounding_set[:]
    for atom1, atom2 in product(grounding_set, grounding_set):
      if atom1 > atom2 and atom1.equal_upto_functional_terms(atom2):
        if atom1.is_functional_atom():
          new_grounding_set.remove(atom1)
        else:
          new_grounding_set.remove(atom2)
    return new_grounding_set
                                           


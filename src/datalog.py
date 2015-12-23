from collections import defaultdict
from asp_generate_test1 import generate_test1
import numpy as np
import operator
import sys
import time
from multiprocessing import Pool


class CompletenessSolver():

  def infer_rule(self,rule,grounding_set):
    head, body = rule.get_tuple()
    body_set   = set(body)
    facts      = set(grounding_set)
    if body_set.issubset(facts):
      return head
    else:
      return None
    
  
  def infer(self, ground_rules):
    grounding_set = self.grounder.grounding_set
    inferred_facts = set()
    for rule in ground_rules:
      inferred_fact = self.infer_rule(rule,grounding_set)
      if inferred_fact:
        inferred_facts.add(inferred_fact)
    return inferred_facts

   
  def __init__(self):
    self.parser = Parser()

  def read_query(self,filename):
    with open(filename, "r") as q_file:
      query_str     = q_file.read().strip(" \n\r\t")
      grounding_set = self.parser.parse_atoms(query_str)
      self.grounder = Grounder(grounding_set)

  def process_rules(self,data):
<<<<<<< HEAD
    inferred = set()
    for indx, line in enumerate(data):
      rule = self.parser.parse_rule(line)
      grounded_rules = self.grounder.ground_rule(rule)
      inferred = inferred.union(self.infer(grounded_rules))
=======
    inferred = 0
    for indx, line in enumerate(data):
      rule = self.parser.parse_rule(line)
      grounded_rules = self.grounder.ground_rule(rule)
      inferred += len(grounded_rules)
>>>>>>> 882b76cb1414e79b191d29d19f6840b7c939213f
    return inferred

  def split_tcs(self,data,k):
    size = len(data)   
    chunk_size = int(np.ceil(size/float(k)))
    chunks = []
    for i in range(k):
      left_bound = i*chunk_size
      if (i+1)*chunk_size >= size:
        right_bound = size
      else:
        right_bound = (i+1)*chunk_size
      chunks.append(data[left_bound:right_bound])
    return chunks


  def read_ASP_TCs(self,filename):
    with open(filename, "r") as tc_file:
      data = tc_file.read().splitlines()
<<<<<<< HEAD
      k = 4
      pool = Pool(k)
      data_list = self.split_tcs(data,k)
      inferred_list = pool.map(self.process_rules, data_list)
      inferred = set()
      for fact_set in inferred_list:
        inferred = inferred.union(fact_set)
=======
      k = 8
      pool = Pool(k)
      data_list = self.split_tcs(data,k)
      inferred = pool.map(self.process_rules, data_list)
>>>>>>> 882b76cb1414e79b191d29d19f6840b7c939213f
      return inferred

class Grounder():

  def __get_typing(self, grounding_set):
    typing = defaultdict(set)
    for atom in grounding_set:
      p, args = atom.get_tuple()
      for indx, arg in enumerate(args):
        typing[(p,indx)].add(arg)
    return typing


  def __init__(self, grounding_set):
    self.grounding_set = grounding_set
    self.typing        = self.__get_typing(grounding_set)
    #TODO introduce grounding types ... 

  def update_inverse(self,inverse,atom):
    p, args = atom.get_tuple()
    for index, arg in enumerate(args):
      if arg.isupper():
        inverse[arg].add((p,index))

  def create_rule_grounding_substitutions(self, inverse):
    typing = self.typing
    possible_values = defaultdict(set)
    for var,vartypes in inverse.items():
      for vartype in vartypes:
        if not possible_values[var]:
          possible_values[var] = typing[vartype]
        else:
          possible_values[var].intersection_update(typing[vartype])
    substitutions = self.translate_possible_values_into_substitutions(possible_values)
    return substitutions

  def translate_possible_values_into_substitutions(self,possible_values):
    variables = list(possible_values.keys())
    sub_num   = 1
    lens      = map(len,list(possible_values.values()))
    for sub_len in lens:
      sub_num *= sub_len
    subs = np.ndarray(shape=(sub_num,len(variables)), dtype='|S6')
    for var_index,var in enumerate(variables):
      vals = possible_values[var]
      num_of_vals = len(vals)
      for index, val in enumerate(vals):
        from_indx = index*sub_num/num_of_vals
        to_indx   = (index+1)*sub_num/num_of_vals
        subs[from_indx:to_indx,var_index] = val
    return subs, variables

  def apply_subs(self, rule, substitutions, variables):
    grounded_rules = []
    for sub in substitutions:
      grounded_rule = self.apply_substitution(rule,sub,variables)
      grounded_rules.append(grounded_rule)
    return grounded_rules

  def apply_substitution(self,rule,sub,variables):
    head, body = rule.get_tuple()
    grounded_head = self.apply_atom_substitution(head, sub, variables)
    grounded_body = []
    for atom in body:
      grounded_atom = self.apply_atom_substitution(atom, sub, variables)
      grounded_body.append(grounded_atom)
    return Rule(grounded_head,grounded_body)

  def apply_atom_substitution(self, atom, sub, variables):
    p, args = atom.get_tuple()
    new_args = args[:]
    for pos,arg in enumerate(args):
      try:
        index = variables.index(arg)
        new_args[pos] = sub[index].decode('UTF-8')
      except:
        continue
    return Atom(p,new_args)


  def ground_rule(self, rule):
    inverse = defaultdict(set)
    head, body = rule.get_tuple() #do not apply inverse to the head, 
    for atom in body:
      self.update_inverse(inverse,atom)
    substitutions, variables = self.create_rule_grounding_substitutions(inverse)
    grounded_clauses = self.apply_subs(rule, substitutions, variables)
    return grounded_clauses

class Parser():

  def parse_atom(self,input_str):
    atom_str = input_str.strip()
    pred, rest = atom_str.split("(")
    rest = rest[:-1] # removed ')' at the end 
    args = rest.split(",")
    args = [arg.strip() for arg in args]
    return Atom(pred,args)

  def parse_atoms(self,input_str):
    atoms_str = input_str.strip(". ")
    atoms_raw = atoms_str.split(".")
    atoms = []
    for atom in atoms_raw:
      atoms.append(self.parse_atom(atom))
    return atoms
  

  def parse_rule(self,input_str):
    rule_str = input_str.strip(" .")
    head, rest = rule_str.split(":-")
    head = self.parse_atom(head.strip())
    body = []
    while rest:
      atom_index   = rest.index(")")
      current_atom = self.parse_atom(rest[:atom_index+1].strip())
      rest         = rest[atom_index+1:].strip(", ")
      body.append(current_atom)
    return Rule(head, body)

class Rule():

  def __init__(self, head, body):
    self.head = head
    self.body = body
    self.__out_str = str(head) + " :- " + ",".join(map(lambda x: str(x),body)) + "."

  def __repr__(self):
    return self.__out_str

  def __str__(self):
    return self.__out_str

  def get_tuple(self):
    return self.head, self.body


class Atom():

  def get_tuple(self):
    return self.predicate, self.args[:]
  
  def __precompute_repr(self,pred,args):
    return pred +"(" + ",".join(args) + ")"

  def __init__(self, pred_name, args):
    self.predicate = pred_name
    self.args      = args[:]
    self.__out_str   = self.__precompute_repr(pred_name,args)

  def __eq__(self, another_atom):
    return self.__out_str == another_atom.__out_str
  
  def __hash__(self):
    return hash(self.__out_str)

  def __repr__(self):
    return self.__out_str

  def __str__(self):
    return self.__out_str

def run_test1():
  t0 = time.time()
  solver = CompletenessSolver()
  experiment_folder = "../experiments/test1/"
  solver.read_query(experiment_folder+"query")
  grounded_tcs = solver.read_ASP_TCs(experiment_folder+"tcs")
  print(grounded_tcs)
  t1 = time.time()
  total_n = t1-t0
  print("Total seconds {}".format(str(total_n)))

def main():
<<<<<<< HEAD
  C=1000 
  S=10000
  print('generating C={C}, S={S}'.format(C=C,S=S))
  generate_test1(C,S)
  print('running')
=======
>>>>>>> 882b76cb1414e79b191d29d19f6840b7c939213f
  run_test1()
 
if __name__ == "__main__":
  main()

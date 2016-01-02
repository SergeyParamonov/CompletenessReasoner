from collections import defaultdict
from test_generators.test1 import generate_test1
import operator
import sys
import time
from multiprocessing import Pool
from itertools import product
import math


class CompletenessSolver():

  num_of_cores_to_use = 4
  
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
      inferred = inferred.union(self.infer(grounded_rules))
    grounding_set = list(inferred.union(set(grounding_set)))
    grounding_set = self.clean_after_FK(grounding_set)
    self.grounder = Grounder(grounding_set)

  def check_query(self):
    self.read_query(self.query_file)
    fks_a = [] # default empty rules for fk_a
    if self.fk_file and self.fk_semantics:
      fks_i, fks_a = self.read_FKs()
      self.update_grounding_set(fks_i)
    inferred_available = self.infer_TCs(self.tcs_file,FK_rules=fks_a)
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


  def infer_TCs(self,filename,FK_rules = []):
    with open(filename, "r") as tc_file:
      data = tc_file.read().splitlines() + list(map(lambda x: str(x),FK_rules))
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
      if (term1 != term2) and not (Grounder.is_functional_term(term1) or Grounder.is_functional_term(term2)):
        return False
    return True
  
  @staticmethod
  def is_functional_atom(atom):
    p, args = atom.get_tuple()
    for term in args:
      if Grounder.is_functional_term(term):
        return True
    return False
                                           
  def clean_after_FK(self,grounding_set):                
    grounding_set     = grounding_set[:]
    new_grounding_set = grounding_set[:]
    for atom1, atom2 in product(grounding_set, grounding_set):
      print(atom1,atom2)
      if atom1 > atom2 and self.equal_upto_functional_terms(atom1,atom2):
        if self.is_functional_atom(atom1):
          print("REMOVED", atom1)
          new_grounding_set.remove(atom1)
        else:
          print("REMOVED", atom2)
          new_grounding_set.remove(atom2)
    return new_grounding_set
                                           
                                           
class Grounder():                          
                                           
  @staticmethod                            
  def is_functional_term(term):            
    if "[" in term:                        
      return True                          
    else:                                  
      return False                         
                                           
  def __get_typing(self, grounding_set):   
    typing = defaultdict(set)              
    for atom in grounding_set:             
      p, args = atom.get_tuple()           
      for indx, arg in enumerate(args):    
      # if not Grounder.is_functional_term(arg):
        typing[(p,indx)].add(arg)          
    return typing                          
                                           
                                           
  def __init__(self, grounding_set):       
    self.grounding_set = grounding_set     
    self.typing        = self.__get_typing(grounding_set)
    #TODO introduce grounding types ...    
                                           
  def update_inverse(self,inverse,atom):   
    p, args = atom.get_tuple()             
    for index, arg in enumerate(args):     
      if arg[0].isupper():                 
        inverse[arg].add((p,index))        
                                           
  def create_rule_grounding_substitutions  (self, inverse):
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
    subs = product(*[possible_values[var] for var in variables])
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
      if not Grounder.is_functional_term(arg):
        try:
          index = variables.index(arg)
          new_args[pos] = sub[index]
        except:
          continue
      else:
        new_args[pos] = self.apply_functional_substitution(arg, sub, variables)
    return Atom(p,new_args)

  @staticmethod
  def apply_functional_substitution(term, sub, variables):
    # cut the fun name
    f_name_indx = term.index("[")
    f_name = term[:f_name_indx]
    f_vars = term[f_name_indx+1:-1].split(";") 
    f_vars = [var.strip() for var in f_vars]
    
    new_f_vars = []
    for var in f_vars:
     index = variables.index(var)
     new_f_vars.append(sub[index])

    
    ground_f_atom = f_name + "[" + ';'.join(new_f_vars) + "]"
    return ground_f_atom


  def ground_rule(self, rule):
    inverse = defaultdict(set)
    head, body = rule.get_tuple() #do not apply inverse to the head, 
    for atom in body:
      self.update_inverse(inverse,atom)
    substitutions, variables = self.create_rule_grounding_substitutions(inverse)
    grounded_clauses = self.apply_subs(rule, substitutions, variables)
    return grounded_clauses

class Parser():

  def parse_FK(self, input_str, enforced_semantics = False):
    from_atom, to_atom, from_indeces, to_indeces = map(lambda x: x.strip(),input_str.split(";"))
    from_indeces = eval(from_indeces)
    to_indeces   = eval(to_indeces)
    from_atom = self.parse_atom(from_atom)
    to_atom   = self.parse_atom(to_atom)
    from_p,from_args = from_atom.get_tuple()
    to_p,  to_args   = to_atom.get_tuple()
    var_name         = "X_"
    new_from_args = from_args[:]
    new_to_args   = to_args[:]
    FK_key_args   = []
    for Indx,(I1,I2) in enumerate(zip(from_indeces,to_indeces),start=1):
      fresh_var = var_name + str(Indx)
      new_from_args[I1-1] = fresh_var
      new_to_args[I2-1]   = fresh_var
      FK_key_args.append(fresh_var)
   
    fun_name = "f_"
    for I,_ in enumerate(to_args,start=1):
      if I not in to_indeces:
        new_to_args[I-1] = fun_name + str(I) + "["+ ";".join(FK_key_args) + "]"


    fresh_name = "YYY_"

    for I,_ in enumerate(from_args,start=1):
      if I not in from_indeces:
        new_from_args[I-1] = fresh_name + str(I)
      
    new_to_atom = Atom(to_p, new_to_args)
    new_from_atom = Atom(from_p, new_from_args)

    i_rule = Rule(new_to_atom, [new_from_atom]) 

    if enforced_semantics:
      enforced_rule = self.create_enforced_rule(new_from_atom,new_to_atom)
      return i_rule, enforced_rule
    else:
      return i_rule, None

  def create_enforced_rule(self, new_from_atom, new_to_atom):
    p, args = new_to_atom.get_tuple()
    fresh_var = "ZZZ_"
    non_functional_args = args[:]
    for indx, arg in enumerate(args):
      if Grounder.is_functional_term(arg):
        non_functional_args[indx] = fresh_var + str(indx)
    head = Atom(p +"_a", non_functional_args)
    body_to = Atom(p, non_functional_args)
    return Rule(head,[new_from_atom,body_to])

  def parse_atom(self,input_str):
    atom_str = input_str.strip()
    pred, rest = atom_str.split("(")
    rest = rest[:-1] # removed ')' at the end 
    args = rest.split(",")
    args = [arg.strip() for arg in args]
    return Atom(pred,args)

  def parse_atoms(self,input_str):
    atoms_str = input_str.strip(". ")
    atoms_raw = atoms_str.split(";")
    atoms = []
    for atom in atoms_raw:
      atoms.append(self.parse_atom(atom.lower()))
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

  def __gt__(self, another_atom):
    return self.__out_str > another_atom.__out_str

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
  C=100
  S=100
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

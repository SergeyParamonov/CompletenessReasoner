#datalog classes
from atom import Atom
from rule import Rule
from CFDC import CFDC

#enable || parsing of TCs
from multiprocessing import Pool

from itertools import chain
import math

class Parser():

  num_of_cores_to_use = 8

  @staticmethod
  def parse_FK(input_str, enforced_semantics = False):
    from_atom, to_atom, from_indeces, to_indeces = map(lambda x: x.strip(),input_str.split(";"))
    from_indeces = eval(from_indeces)
    to_indeces   = eval(to_indeces)
    from_atom = Parser.parse_atom(from_atom)
    to_atom   = Parser.parse_atom(to_atom)
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
      enforced_rule = Parser.create_enforced_rule(new_from_atom,new_to_atom)
      return i_rule, enforced_rule
    else:
      return i_rule, None

  @staticmethod
  def create_enforced_rule(new_from_atom, new_to_atom):
    p, args = new_to_atom.get_tuple()
    p_from, args_from = new_from_atom.get_tuple()
    p_from_a = p_from + "_a"
    from_atom_available = Atom(p_from_a, args_from)
    fresh_var = "ZZZ_"
    non_functional_args = args[:]
    for indx, arg in enumerate(args):
      if Atom.is_functional_term(arg):
        non_functional_args[indx] = fresh_var + str(indx)
    head = Atom(p +"_a", non_functional_args)
    body_to = Atom(p, non_functional_args)
    return Rule(head,[from_atom_available,body_to])

  @staticmethod
  def parse_atom(input_str):
    atom_str = input_str.strip()
    pred, rest = atom_str.split("(")
    rest = rest[:-1] # removed ')' at the end 
    args = rest.split(",")
    args = [arg.strip() for arg in args]
    return Atom(pred,args)

  @staticmethod
  def parse_atoms(input_str):
    atoms_str = input_str.strip(". ")
    atoms_raw = atoms_str.split(";")
    atoms = []
    for atom in atoms_raw:
      atoms.append(Parser.parse_atom(atom.lower()))
    return atoms
  

  @staticmethod
  def parse_rule(input_str):
    rule_str = input_str.strip(" .")
    head, rest = rule_str.split(":-")
    head = Parser.parse_atom(head.strip())
    body = []
    while rest:
      atom_index   = rest.index(")")
      current_atom = Parser.parse_atom(rest[:atom_index+1].strip())
      rest         = rest[atom_index+1:].strip(", ")
      body.append(current_atom)
    return Rule(head, body)

  

  
  @staticmethod
  def parse_CFDC(input_str): 
    escaped_str = input_str.replace(" ","")
    R, i1, v1, i2, v2 = escaped_str.split(";")
    i1 = int(i1)
    if i2 != "-" and v2 != "-":
      i2 = int(i2)
    else:
      i2 = None
      v2 = None
    v1 = v1.replace("{","")
    v1 = v1.replace("}","")
    v1 = set(v1.split(","))
    return CFDC(R,i1,v1,i2,v2)

  @staticmethod
  def parse_CFDCs(filename):
    with open(filename,"r") as cfdc_file:
      cfdcs = []
      lines = cfdc_file.read().splitlines()
      for line in lines:
        cfdc = Parser.parse_CFDC(line)
        cfdcs.append(cfdc)
      return cfdcs

  @classmethod
  def parse_rules(cls,list_of_lines):
   return [cls.parse_rule(line) for line in list_of_lines]

  @staticmethod
  def read_tcs(filename):
    with open(filename, "r") as tc_file:
      lines = tc_file.read().splitlines()
      k     = Parser.num_of_cores_to_use
      pool  = Pool(k)
      data_list = Parser.split_into_k(lines,k)
      parsed_rule_list = pool.map(Parser.parse_rules, data_list)
      parsed_rules = list(chain(*parsed_rule_list))
      return parsed_rules

  @staticmethod
  def split_into_k(data,k):
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





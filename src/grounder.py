# datalog classes
from atom import Atom
from rule import Rule
from parser import Parser

from collections import defaultdict
from itertools import product

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
      if arg[0].isupper():                 
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
      if not Atom.is_functional_term(arg):
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

  def ground_rules(self, rules):
    grounded_rules = []
    for rule in rules:
      grounded_rules += self.ground_rule(rule)
    return grounded_rules

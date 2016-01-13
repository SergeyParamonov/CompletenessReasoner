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

  @staticmethod                            
  def is_functional_term(term):            
    if "[" in term:                        
      return True                          
    else:                                  
      return False  

  @staticmethod
  def substitute_functional_term(term, case_sub, case_vars):
    left_braket   = term.index("[")
    right_braket  = term.index("]")
    variables     = term[left_braket+1:right_braket].split(";")
    new_variables = [Atom.substitute_var(var,case_sub,case_vars) for var in variables] 
    new_term      = term[:left_braket+1] + ";".join(new_variables) + term[right_braket:]
    return new_term

  @staticmethod
  def substitute_var(var, case_sub, case_vars):
    if var in case_vars:
      return case_sub[var]
    else:
      return var


  def get_case_substituted(self, case_sub):
    case_vars = list(case_sub.keys())
    new_args = []
    for arg in self.args:
      if Atom.is_functional_term(arg):
        new_args.append(Atom.substitute_functional_term(arg,case_sub,case_vars))
      else:
        new_args.append(Atom.substitute_var(arg,case_sub,case_vars))

    return Atom(self.predicate, new_args)

  def equal_upto_functional_terms(self, another_atom):
    p1, args1 = self.get_tuple()
    p2, args2 = another.get_tuple()
    if p1 != p2:
      return False
    for term1, term2 in zip(args1,args2):  
      if (term1 != term2) and not (Atom.is_functional_term(term1) or Atom.is_functional_term(term2)):
        return False
    return True

  
  def is_functional_atom(self):
    p, args = self.get_tuple()
    for term in args:
      if Atom.is_functional_term(term):
        return True
    return False


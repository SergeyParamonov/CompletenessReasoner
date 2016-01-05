#datalog classes
from atom import Atom
from rule import Rule

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
    p_from, args_from = new_from_atom.get_tuple()
    p_from_a = p_from + "_a"
    from_atom_available = Atom(p_from_a, args_from)
    fresh_var = "ZZZ_"
    non_functional_args = args[:]
    for indx, arg in enumerate(args):
      if self.is_functional_term(arg):
        non_functional_args[indx] = fresh_var + str(indx)
    head = Atom(p +"_a", non_functional_args)
    body_to = Atom(p, non_functional_args)
    return Rule(head,[from_atom_available,body_to])

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

  @staticmethod                            
  def is_functional_term(term):            
    if "[" in term:                        
      return True                          
    else:                                  
      return False  

  @staticmethod
  def is_functional_atom(atom):
    p, args = atom.get_tuple()
    for term in args:
      if Parser.is_functional_term(term):
        return True
    return False


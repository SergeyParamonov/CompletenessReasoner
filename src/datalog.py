from collections import defaultdict

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
    return possible_values

  def ground_rule(self, rule):
    inverse = defaultdict(set)
    head, body = rule.get_tuple()
    self.update_inverse(inverse, head)
    for atom in body:
      self.update_inverse(inverse,atom)
    substitutions = self.create_rule_grounding_substitutions(inverse)
    return substitutions


    #TODO use grounding set to ground atom


class Parser():
  def parse_atom(self,input_str):
    atom_str = input_str.strip()
    pred, rest = atom_str.split("(")
    rest = rest[:-1] # removed ')' at the end 
    args = rest.split(",")
    return Atom(pred,args)

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

  def __repr__(self):
    return self.__out_str

  def __str__(self):
    return self.__out_str


def main():

  p  = Parser()
  g  = Grounder([a,a2,b,g,g2])
  print(g.grounding_set)
  print(g.typing)
  r  = p.parse_rule("a(X,Y) :- b(Z,Y), g(Y,X), g(X,Y)..")
  print(r)
  print(g.ground_rule(r))
# print(r)


if __name__ == "__main__":
  main()

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



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




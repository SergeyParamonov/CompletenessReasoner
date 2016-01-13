from grounder import Grounder 

class Chunk():
  def __init__(self, values, fdc_vars, tcs, grounder, solver, fks_a=None):
    self.values       = values
    self.fdc_vars     = fdc_vars
    self.tcs          = tcs
    self.substitution = dict(zip(fdc_vars, values))
    self.grounder     = grounder
    self.fks_a        = fks_a
    self.solver       = solver

  def __repr__(self):
    return str(self.substitution)

  def __str__(self):
    return self.__repr__()


  def process_chunk(self):
    #TODO fix process rules
    inferred_available = self.solver.process_rules(self.tcs, grounder=self.grounder, case_sub=self.substitution)

    if self.fks_a:
      inferred_available = self.solver.apply_fks_a(fks_a, inferred, case_sub=self.substitution)

    q_a_grounder   = Grounder(inferred_available)
    grounded_rules = q_a_grounder.ground_rule(self.solver.q_a)
    q_a            = self.solver.infer(grounded_rules,grounding_set=inferred_available, case_sub=self.substitution)
    return q_a, inferred_available



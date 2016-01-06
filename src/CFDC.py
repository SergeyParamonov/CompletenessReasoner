class CFDC():
  def __init__(self,pred_name,indx1,vals1,indx2,val2):
    self.pred_name = pred_name
    self.indx1     = indx1
    self.vals1     = vals1
    self.indx2     = indx2
    self.val2      = val2
  
  def __repr__(self):
    if self.indx2 and self.val2:
      out_str = "CFDC({pred};{i1};{vals1};{i2};{val2})".format(pred=self.pred_name,i1=self.indx1,vals1=",".join(self.vals1),i2=self.indx2,val2=self.val2)
    else:
      out_str = "FDC({pred};{i};{vals})".format(pred=self.pred_name,i=self.indx1,vals=",".join(self.vals1))
    return out_str

  def __str__(self):
    return self.__repr__()

  def get_fdc_tuple(self):
    return self.pred_name, self.indx1, self.vals1

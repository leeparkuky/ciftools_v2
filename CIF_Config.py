from dataclasses import dataclass
from typing import Union, List
from utils import *





@dataclass
class ACSConfig:
    year: Union[str, int]
    state_fips: Union[str, int, List[str], List[int]]
    query_level: str        
    acs_group: str
    acs_type: str = None
    
    
    def reset_attributes(self):
        delattr(self, '_labels'); delattr(self,'_variables'); delattr(self,'var_desc')
        
    
    def raise_for_status(self, groups:List[str] = None)-> bool:
        if groups is None:
            if hasattr(self, '_all_groups'):
                groups = self._all_groups
            else:
                groups = gen_group_names_acs(self)
            
        if self.acs_group not in groups:
            raise AttributeError("Check your ACSConfig attributes")
        else:
            pass
        
    def find_acs_type(self):
        self.acs_type = check_acs_type(self)
    
    @property
    def labels(self):
        if hasattr(self, "_labels"):
            pass
        else:
            if hasattr(self, '_variables'):
                pass
            else:
                self.variables
            label_col = self.var_desc.loc[self.var_desc.name.isin(self.variables),:].sort_values('name').label.reset_index(drop = True)
            labels  = [x[-2].replace(":",'').replace("Estimate!!","") + 
                       " - " + 
                       x[-1].replace(":",'').replace("Estimate!!","") if len(
                           x) > 2 else x[-1].replace(":",'').replace("Estimate!!","") for x in label_col.str.split(":!!")]
            self._labels = labels
        return self._labels
    
    
    
    @property
    def variables(self):
        if hasattr(self, "_variables"):
            pass
        else:
            if self.acs_type == None:
                self.find_acs_type()
            res = gen_variable_names(self.year, self.acs_type, self.acs_group)
            self._variables = res[0]
            self.var_desc = pd.DataFrame(res[1][1:], columns = res[1][0])
        return self._variables

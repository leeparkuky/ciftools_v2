from dataclasses import dataclass
from typing import Union, List
from utils import *


@dataclass
class SocrataConfig:
    domain: str
    app_token: str
    user_name: str = None
    password : str = None
    
    @property
    def client(self):
        from sodapy import Socrata
        if hasattr(self, '_client'):
            pass
        else:
            if self.user_name:
                client = Socrata(self.domain,
                      self.app_token,
                      username=self.user_name,
                      password=self.password)
            else:
                client = Socrata(self.domain, self.app_token)
            self._client = client
        return self._client


@dataclass
class ACSConfig:
    year: Union[str, int]
    state_fips: Union[str, int, List[str], List[int]]
    query_level: str        
    acs_group: Union[str, List[str]]
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
            
            def join_labels(seq, num_labels):
                text = seq[-num_labels].replace(":",'').replace("Estimate!!","") 
                for i in range(num_labels-1):
                    text += ' - '
                    text += seq[-num_labels + i + 1].replace(":",'').replace("Estimate!!","")
                return text
            
            labels  = []

            for x in label_col.str.split(":!!"):
                if len(x) <= 2:
                    labels.append(x[-1].replace(":",'').replace("Estimate!!",""))
                else:
                    labels.append(join_labels(x, num_labels = len(x)))

            self._labels = labels

        return self._labels
    
    @property
    def var_desc(self):
        if hasattr(self, '_var_desc'):
            pass
        else:
            self.varaibels
        return self._var_desc
    

    @property
    def variables(self):
        if hasattr(self, "_variables"):
            pass
        else:
            if self.acs_type == None:
                self.find_acs_type()
            res = gen_variable_names(self.year, self.acs_type, self.acs_group)
            self._variables = res[0]
            self._var_desc = pd.DataFrame(res[1][1:], columns = res[1][0])
            self._var_desc = self.var_desc.loc[self.var_desc.name.isin(self._variables),:].reset_index(drop = True)
                
        return self._variables

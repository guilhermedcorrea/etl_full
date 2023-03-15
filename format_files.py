from dataclasses import dataclass, asdict,field
from collections.abc import Sequence
from datetime import date
import re
from typing import Any, Literal
from dataclasses import dataclass, asdict,field

@dataclass
class CalculateValues(Sequence):
    venda_id: int
    venda_numeracao: int
    total_frete: float
    pessoa_id: int
    id_produto: int
    idstatus: int
    loja_id: int
    datacadastro: str
    total_venda: float
    nome_status: str
    nome_status: str
    valor_unitario: float
    custo_unitario: float
    quantidade: int = 0
    margem: float = 0
    total: float = 0
    quant_vtotal: float = 0
    _data = {}
    _index = 0
    _next_index = 0

 
    def __len__(self) -> int:
        return self._index

    def __getitem__(self, index) -> Any:
        return self._data[index]

    def __setitem__(self, index, value):
        self._data[index] = value
        
    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        if self._next_index >= self._index:
            self._next_index = 0
            raise StopIteration
        value = self._data[self._next_index]
        self._next_index +=1
        return value

    def dict(self):
        return {k: str(v) for k, v in asdict(self).items()}

    @property
    def valor_unitario(self) -> (float | Literal[0]):
        if re.search(r"[-+]?\d*\.?\d+|[-+]?\d+", str(
            self._valor_unitario)) and not re.search(
                '[a-z]+', str(self._valor_unitario), re.IGNORECASE):
            return round(float(self._valor_unitario),2)
        return 0
    
    @valor_unitario.setter
    def valor_unitario(self, value) -> None:
        self._valor_unitario = value

    @property
    def custo_unitario(self) -> (float | Literal[0]):
        if re.search(r"[-+]?\d*\.?\d+|[-+]?\d+", str(
            self._custo_unitario)) and not re.search(
                '[a-z]+', str(self._custo_unitario), re.IGNORECASE):
            return round(float(self._custo_unitario),2)
        return 0

    @custo_unitario.setter
    def custo_unitario(self, value) -> None:
        self._custo_unitario = value
        
 
    def total_cost(self) -> float:
        try:
            self.quant_vtotal = round(float(self._valor_unitario * self.quantidade),2)
            return self.quant_vtotal
        except:
            return 0


    def calculate_margem(self) -> (dict | Literal[0]):
  
        try: 
           self.margem = round(float((self._valor_unitario / self._custo_unitario -1) * 100),2)
           return self.margem 

        except:
            return 0
'''      
    def format_dicts_oders(self):
        print('margem',self.margem, self.quantidade, self.name, self.valor_unitario, self.custo_unitario)

'''


def select_produtos():
    




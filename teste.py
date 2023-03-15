from dataclasses import dataclass
from datetime import date
import re

@dataclass
class CalculateValues:
    name: str
    unit_price: float
    cust_price: float
    margin: float = 0
    quantity_on_hand: int = 0

    @property
    def unit_price(self):
        if re.search(r"[-+]?\d*\.?\d+|[-+]?\d+", str(
            self._cust_price)) and not re.search(
                '[a-z]+', str(self._unit_price), re.IGNORECASE):
            return round(float(self._unit_price),2)
        return 0
    
    @unit_price.setter
    def unit_price(self, value):
        self._unit_price = value

    @property
    def cust_price(self):
        if re.search(r"[-+]?\d*\.?\d+|[-+]?\d+", str(
            self._cust_price)) and not re.search(
                '[a-z]+', str(self._cust_price), re.IGNORECASE):
            return round(float(self._cust_price),2)
        return 0

    @cust_price.setter
    def cust_price(self, value):
        self._cust_price = value
        
    @property
    def name(self):
        print(self._name)
        return self._name
    
    @name.setter
    def name(self, value):
        self._name = value.capitalize()

    def total_cost(self) -> float:
        return self.unit_price * self.quantity_on_hand


    def calculate_margem(self):
        try:
            self.margin = round((self._unit_price / self.cust_price -1) * 100,2)
            return self.margin
        except ZeroDivisionError:
            return 0
        
    
list_dicts = []

teste = CalculateValues('teste produto', unit_price = 10.2, quantity_on_hand = 5,cust_price= 8.0)
teste.calculate_margem()
teste.total_cost()

new_dict = teste.__dict__
list_dicts.append(new_dict)
print(list_dicts)



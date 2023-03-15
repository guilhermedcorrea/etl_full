from abc import ABC, abstractmethod
import pandas as pd
import schedule
import time
from functools import wraps
from config import mssql_get_conn as myboxenginesql
from sqlalchemy import text
from sqlalchemy import insert, select
from typing import Any, Literal, Dict
from itertools import zip_longest, chain
from collections import ChainMap
from insert import (insert_dim_pedidos,insert_ambientados,insert_dim_status,insert_fabricante,insert_fato,insert_dim_produtos
        ,insert_custo_produtos, insert_dim_pessoas,insert_dim_notas_fiscais
        , insert_categorias,insert_marca,insert_cidade,insert_frequencia_vendas, insert_dim_unidade)
import numpy as np
from controllers import create_dict_orders



def call_vendas_detalhes():
    dict_tems = create_dict_orders()
    yield dict_tems
            


def get_date(date) -> Any:
    return date

class Tables(ABC):
    @abstractmethod
    def get_values_dabatases(self) -> None:
        ...

class Orders(Tables):
    def get_values_dabatases(self) -> None:
        vendas = call_vendas_detalhes()
        orders = [{**order} for order in chain.from_iterable(vendas)]
        #insert_dim_pedidos(*orders)
        data = pd.DataFrame(orders)
        #data[['bitambientado', 'vendaId', 'produtoId','unidadeId', 
        #'statusId', 'enderecoId','numero']] = data[['bitambientado', 'vendaId', 
        #'produtoId','unidadeId', 'statusId', 'enderecoId','numero']].applymap(lambda k: int(k) if k != None else k)

    
        data['dataCadastro'] = pd.to_datetime(data['dataCadastro'],errors='coerce')
        data['dataAlteracao'] = pd.to_datetime(data['dataAlteracao'],errors='coerce')
        data['dataEstimadaEntrega']  = pd.to_datetime(data['dataEstimadaEntrega'],errors='coerce')
        data['dataEstimadaMontagem'] = pd.to_datetime(data['dataEstimadaMontagem'],errors='coerce')
        data['dataAbertura'] = pd.to_datetime(data['dataAbertura'],errors='coerce')
        data['dataContrato'] = pd.to_datetime(data['dataContrato'],errors='coerce')

        print(data)
        new_dicts = data.to_dict('records')
       
        insert_dim_pedidos(*new_dicts)
  

class Ambientados(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_ambientados
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            
            item = get_ambientados()
            get_items = conn.execute(item).all()
            dict_tems = [row._asdict() for row in get_items]
           
            insert_ambientados(*dict_tems)


class Status(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_status
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            item = get_status()
            get_items = conn.execute(item).all()
            dict_tems = [row._asdict() for row in get_items]
            insert_dim_status(*dict_tems)
        
            #insert_ambientados(*dict_tems)
      

class CustoProdutos(Tables):
    
    def get_values_dabatases(self) -> None:
        from querys import get_products_prices
    
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_products_prices()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            df = pd.DataFrame(dict_tems)
            df['dataAlteracao'] = pd.to_datetime(df['dataAlteracao'])
            df['datacadastrodw'] = pd.to_datetime(df['datacadastrodw'])
            dict_preco = df.to_dict('records')
           
            insert_custo_produtos(*dict_preco)
            
     
class Clientes(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_clientes
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_clientes()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            print(dict_tems)
            insert_dim_pessoas(*dict_tems)


class Fabricante(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_fabricante
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_fabricante()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            dataframe = pd.DataFrame(dict_tems)
            dataframe['NomeFabricante'] = dataframe['NomeFabricante'].apply(
                lambda k: k.strip().capitalize())
            
            dataframe['slug'] = dataframe['slug'].apply(lambda k: str(k).replace("-","").strip().capitalize())

            dict_fabrica = dataframe.to_dict('records')
            insert_fabricante(*dict_fabrica)
         

class Produtos(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_produtos_cadastros
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_produtos_cadastros()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            insert_dim_produtos(*dict_tems)


class NotasFiscais(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_notas
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_notas()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            df = pd.DataFrame(dict_tems)
            df['dataCadastro'] =  pd.to_datetime(df['dataCadastro'])
            df['dataAtualizacao'] = pd.to_datetime(df['dataAtualizacao'])
            dict_nf = df.to_dict("records")
        
            insert_dim_notas_fiscais(*dict_nf)
          
         

class FrequenciaVenda(Tables):
     def get_values_dabatases(self) -> None:
        from querys import get_frequencia_venda

        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_frequencia_venda()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            insert_frequencia_vendas(*dict_tems)


class Categorias(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_produtos_cadastros
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_produtos_cadastros()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            df = pd.DataFrame(dict_tems)
            categoria = df[['categoriaId','nomecategoria']]
            categoria = categoria.drop_duplicates()
            dict_df = categoria.to_dict('records')
            insert_categorias(*dict_df)


class Marcas(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_fabricante
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_fabricante()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
     
         
            insert_marca(*dict_tems)


class Cidade(Tables):
       def get_values_dabatases(self) -> None:
        from querys import get_enderecos_client
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_enderecos_client()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            df = pd.DataFrame(dict_tems)
            df = df[['cidade','idendereco','uf']]
            df['idendereco'] = df['idendereco'].astype(int)

            df = df.drop_duplicates()
            dicts = df.to_dict('records')
     
            insert_cidade(*dicts)

       
class Lojas(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_unidades
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            items = get_unidades()
            call = conn.execute(items).all()
            dict_tems = [row._asdict() for row in call]
            insert_dim_unidade(*dict_tems)


class Categorias(Tables):
    from querys import get_categorias
    enginemssql = myboxenginesql()
    with enginemssql.begin() as conn:
        items = get_categorias()
        call = conn.execute(items).all()
        dict_tems = [row._asdict() for row in call]
        insert_categorias(*dict_tems)
        








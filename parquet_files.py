from datetime import datetime
import os
from typing import Any
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import re
from insert_tables import inserted_dim_pedidos


class SaveItens:
    def __init__(self, path):
        self.data = str(datetime.now()).split(" ")[0]
        self.path = path

    def save_orders_file(self):
        ...


    def convert_values_int(self, values):
        try:
            item = list(filter(
                lambda k: int(str(k).split(".0")) if '.0' == k else k, values))
            return item
        except:
            return "NotFound"


    def generation_path(self, filename =None) -> str:
        name_path = os.path.join(os.path.join(os.path.join(
            os.getcwd(),'files'),'consultas'),self.path)
        new_file = os.path.join(name_path,self.data)
        return new_file

    def create_new_path(*args, **kwargs):
        os.mkdir(kwargs['path'])
       

    def generate_new_dir(self, *args, **kwargs: dict[str, Any]) -> str:
        path = self.path
        newpath = self.generation_path(path)
        name = newpath
        if not os.path.isdir(newpath):
            self.create_new_path(path = newpath)
            return name
        return name

    def save_parquet_file(self, *args, **kwargs):
        data = pd.DataFrame(kwargs['df'])
        data = data.drop_duplicates()
        data.notna()

        data[['id','NumeroContrato','numeracao',
            'lojaid', 'pessoaId', 'statusId']] = data[['id','NumeroContrato','numeracao',
            'lojaid', 'pessoaId', 'statusId']].apply(
                lambda k: k if type(k) == int else self.convert_values_int(k))
        
        data['dataCadastro'] = data['dataCadastro'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%d'))

        data['dataAlteracao'] = data['dataAlteracao'].apply(
            lambda x: pd.Timestamp(x).strftime('%Y-%m-%d'))
        
        valor = kwargs['path'].split("\\")
        x = valor[:-1]
        y = ("\\").join(x)

        filenames = [files for files in os.listdir(y) if files == self.data]
        new_file_partque = kwargs['path']
        new_df = pd.DataFrame(kwargs['df'])
        table = pa.Table.from_pandas(new_df)
        pq.write_table(table, os.path.join(kwargs['path'],str(self.data)+str('.parquet')))

        print(table)
        #print(os.listdir(kwargs['path']) )
    
    def get_dataframe(self, *args, **kwargs):
        file_path = self.generate_new_dir(path = self.path)
        print(file_path)
        self.save_parquet_file(path =file_path, df = args)

    
    def save_parquet_orders_itens(self, *args, **kwargs):
        table = pa.Table.from_pandas(kwargs['df'])
        teste = self.generate_new_dir(kwargs['path'])
      
        name = str(self.data)+str('pedidos_itens.parquet')
       
        new_item = os.path.join(teste,name)
   
        pq.write_table(table, new_item)

        #data = table.to_pandas()

        #dict_itens = data.to_dict('records')
        #print(dict_itens)



def reader_parquet_files():
    orders = r'C:\Users\Guilherme\Documents\etl_my_box\files\consultas\pedidos\2023-02-06\2023-02-06.parquet'
    orders_itens = r'C:\Users\Guilherme\Documents\etl_my_box\files\consultas\pedidos_itens\2023-02-06\2023-02-06pedidos_itens.parquet'
    table2 = pq.read_table(orders)
    dforder = table2.to_pandas()

    table3 = pq.read_table(orders_itens)
    dfitens = table3.to_pandas()
  
    dfitens['valorUnitario'] = dfitens['valorUnitario'].astype(float)

    dfitens['valorUnitarioCusto'] = dfitens['valorUnitarioCusto'].astype(float)

    #dfitens['valorUnitarioCusto'] = dfitens['valorUnitarioCusto'].apply(lambda k: verify_floats(k))

    dfitens['Margem'] = round((dfitens['valorUnitario'] / dfitens['valorUnitarioCusto']-1) * 100,2)

    #dfitens['Margem'] = dfitens['Margem'].apply(lambda k: verify_floats(k))

    df= pd.merge(dfitens, dforder, left_on=['vendaId','unidadeId'], right_on=['id','lojaid'], how='left')
    df['Margem'] = df['Margem'].astype(float)
  
    notna = df[df['lojaid'].notnull()]
    notna = notna.drop_duplicates()
    
    notna = notna[['vendaId', 'produtoId', 'quantidade', 'valorUnitario',
       'valorUnitarioCusto', 'unidadeId', 'id', 'unidade', 'NumeroContrato',
       'totalVenda', 'dataCadastro','numeracao','totalFrete',
        'lojaid', 'pessoaId','totalDesconto']]

    notna['id'] = notna['id'].apply(lambda k: int(str(k).split(".")[0].strip()))
    notna['pessoaId'] = notna['pessoaId'].apply(lambda k: int(str(k).split(".")[0].strip()))
    notna['lojaid'] =notna['lojaid'].apply(lambda k: int(str(k).split(".")[0].strip()))
    notna['produtoId'] =notna['produtoId'].apply(lambda k: int(str(k).split(".")[0].strip()))
    notna['vendaId'] =notna['vendaId'].apply(lambda k: int(str(k).split(".")[0].strip()))

    notna['dataCadastro'] = notna['dataCadastro'].apply(
        lambda x: pd.Timestamp(x).strftime('%Y-%m-%d'))



    dict_orders = notna.to_dict('records')

    inserted_dim_pedidos(*dict_orders)
   
    

   

reader_parquet_files()
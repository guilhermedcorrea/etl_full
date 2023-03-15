import pandas as pd

from config import mssql_get_conn, mssq_datawharehouse
#from mybox_tables import Produto,ProdutoPreco,Venda,VendaItem, Endereco, Pessoa,VendaStatus,Categorias
from mybox_tables import Produto
from sqlalchemy import select, join
from typing import Generator
from sqlalchemy import text, insert, select
import warnings
warnings.filterwarnings('ignore')
from itertools import chain
from mybox_tables import (Produto,ProdutoPreco,Venda,VendaItem
,Endereco, Pessoa,VendaStatus,Categorias, VendaComunicacaoStatus)

from dw_comercial import (DimCidade, DimCliente, DimCustos, DimEndereco
    ,DimLoja, DimMarca, dim_notafiscal, DimPedido, DimProduto, DimStatus)

from insert import insert_dim_produtos, insert_dim_status, insert_dim_custos, insert_enderecos,insert_fabricante

from typing import Any
import re
     
#call = get_detailed_orders('2022-05-01')
#print(call)


def remove_nan(values) -> (Any | None):
    if re.search('\d+', values):
        return values
    else:
         return None



def get_enderecos_client():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        call = (text("""
                select distinct venda.pessoaId, pessoa.id as idcliente, endereco.id as idendereco, endereco.bairro,endereco.uf
                    ,endereco.logradouro,endereco.cidade,endereco.complemento
                    from [myboxmarcenaria].[dbo].[Venda] as venda

                    left join [dbo].[Pessoa] as pessoa
                    on pessoa.id =  venda.pessoaId
                    left join [dbo].[Endereco] endereco
                    on endereco.id = pessoa.enderecoId
                    where pessoa.enderecoId in (
                    SELECT vitem.produtoId
                    from venda VENDAS
                    left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                    left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                    left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId


                    WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2022-07-01'
                    and LOJA.excluido = 0
                    and VENDAS.statusId > 2
                    and VENDAS.excluido = 0
                    and VENDAS.numeracao <> 0)"""))
        exec_ = conn.execute(call).all()
        dict_tems = [row._asdict() for row in exec_]

        insert_enderecos(*dict_tems)

       
#enderecos = get_enderecos_client()

def get_fabricante():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        call = (text("""
            SELECT distinct  fabricante.ID
            ,fabricante.NomeFabricante
            ,fabricante.slug
            ,fabricante.bitPlanejados
            ,fabricante.bitAtivo

            FROM [myboxmarcenaria].[dbo].[Fabricante] as fabricante
            where   fabricante.ID in (
            SELECT fab.ID
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
            left join dbo.Produto as prod on prod.id = vitem.produtoId
            left join dbo.Fabricante as fab on fab.ID = prod.fabricanteId

            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2022-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0)"""))

        exec_ = conn.execute(call)
        dict_tems = [row._asdict() for row in exec_]
        fabricante_df = pd.DataFrame(dict_tems)
        fabricante_df['slug'] = fabricante_df['slug'].apply(
            lambda k: str(k).replace("_","").replace("-","").strip().capitalize())
        fabricante_df = fabricante_df.drop_duplicates()
        dict_fabricante = fabricante_df.to_dict('records')
        insert_fabricante(*dict_fabricante)
        
    
     
get_fabricante()



def pedidos_itens():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('VendaItem', conn)
        df['dataCadastro'] = df['dataCadastro'].apply(
            lambda x: pd.Timestamp(x).strftime('%Y-%m-%d'))
      
        vendaitem = df[['vendaId','produtoId','quantidade','valorUnitario'
            ,'total','dataCadastro','valorUnitarioCusto','valorFrete']]
        vendaitem = vendaitem.drop_duplicates()
        vendaitem[['valorUnitario'
            ,'total','dataCadastro','valorUnitarioCusto','valorFrete']]=vendaitem[['valorUnitario'
            ,'total','dataCadastro','valorUnitarioCusto','valorFrete']].fillna(0)

        return vendaitem


def products_precos():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('ProdutoPreco', conn)
        produto_preco = df[['unidadeId','produtoId','precoVenda','custo','descontoMaximoRecomendado'
            ,'royalties','comissaoVendedor','comissaoArquiteto','precoCompletoSemComissao']]
        produto_preco = produto_preco.drop_duplicates()
        produto_preco['totalFrete'] = produto_preco['totalFrete'].apply(lambda k: remove_nan(k))
        produto_preco['descontoMaximoRecomendado'] = produto_preco['descontoMaximoRecomendado'].apply(lambda k: remove_nan(k))
        produto_preco['comissaoVendedor'] = produto_preco['comissaoVendedor'].apply(lambda k: remove_nan(k))
        produto_preco['royalties'] = produto_preco['royalties'].apply(lambda k: remove_nan(k))
        produto_preco['precoCompletoSemComissao'] = produto_preco['royalties'].apply(lambda k: remove_nan(k))
        produto_preco_dict = produto_preco.to_dict('records')

        #insert_dim_custos(*produto_preco_dict)
        yield produto_preco_dict
      

products_precos()

def get_status():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df_status= pd.read_sql('VendaComunicacaoStatus', conn)
        dict_status = df_status.to_dict('records')
    
        insert_dim_status(*dict_status)
        

'''

def products_values():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('Produto', conn)
   
        df[['Largura', 'Altura'
            , 'Profundidade','Peso','prazoEntregaMedio']] = df[['Largura', 'Altura'
            , 'Profundidade','Peso','prazoEntregaMedio']].fillna(0)

        df['nome'] = df['nome'].apply(lambda k:  str(k).strip().capitalize())

        produtos = df[['id', 'nome','categoriaId','marca','precoCatalogoCusto', 'prazoEntregaMedio', 'fabricanteId',
            'codigoFabricante', 'CodigoBarras', 'Largura', 'Altura', 'Profundidade',
            'Peso', 'NCM']]
        return produtos
      

def get_pessoa():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('Pessoa', conn)
        pessoa = df[['id', 'nomeCompletoRazaoSocial', 'nomeFantasiaApelido', 'ativo',      
                'cpfCnpj', 'enderecoId', 'tipoId',
                'inscricaoMunicipal', 'pessoafisica', 'responsavel',
                'responsavelTelefone', 'celular', 'telefone', 'rgIe', 'tipoSanguineo',
                'dataCadastro', 'dataNascimento', 'unidadeId',      
                'enderecoEntregaId']]

        pessoa['nomeCompletoRazaoSocial'] = pessoa['nomeCompletoRazaoSocial'].apply(
            lambda k: str(k).strip().capitalize())

        pessoa['dataCadastro'] = pd.to_datetime(df['dataCadastro'], errors='coerce').dt.strftime('%Y-%m-%d')
          

        pessoa['dataNascimento'] = pd.to_datetime(df['dataNascimento'], errors='coerce').dt.strftime('%Y-%m-%d')

        pessoa['enderecoEntregaId'] = pessoa['enderecoEntregaId'].apply(lambda k: str(k).replace(".0","").strip())
        pessoa = pessoa.drop_duplicates()
        dict_pessoa = pessoa.to_dict('records')
        return dict_pessoa


    
#get_pessoa()

def get_status():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('VendaComunicacaoStatus', conn)
        print(df)


def get_endereco():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        df = pd.read_sql('Endereco', conn)
        df['data'] = pd.to_datetime(
            df['data'], errors='coerce').dt.strftime('%Y-%m-%d')
        df[['cidade', 'complemento', 'logradouro',
        'data', 'bairro']] =  df[['cidade', 'complemento', 'logradouro',
        'data', 'bairro']].applymap(lambda k: str(k).strip().capitalize())
        endereco = df[['id', 'cep', 'cidade', 'complemento', 'logradouro', 'numero', 'uf',
                'data', 'bairro']]
       




    
 
def dim_pedidos(*args, **kwargs):
    df_pedidos = pd.DataFrame(args)
    df_pedidos = df_pedidos[['id','pessoaId','totalFrete','valorUnitarioCusto'
            ,'valorFrete','Margem','produtoId','lojaid','quantidade','valorUnitarioCusto']]

    df_pedidos = df_pedidos.drop_duplicates()
    df_pedidos_dicts = df_pedidos.to_dict('records')
    #insert_dim_pedidos(*df_pedidos_dicts)

    



'''
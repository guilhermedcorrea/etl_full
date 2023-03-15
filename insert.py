from sqlalchemy import insert,select
from my_box_comercial_dw import (DimPedido, DimCustos, DimEndereco,
    DimStatus, dim_produtos,DimCliente,ambientados,DimPlanejados
    ,DimFabricante, FatoVenda, dim_notafiscal,dim_categorias,DimMarca,DimCidade,DimFrequencia,DimLoja)
from config import mssq_datawharehouse, mssql_get_conn
from itertools import chain
from datetime import datetime
import pytz
from typing import Any, Text
from sqlalchemy import text




def insert_dim_pedidos(*args: tuple, **kwargs: dict[str, Any]) -> None:
    dwengine = mssq_datawharehouse()
    for arg in args:
        print(arg)

        with dwengine.connect() as conn:
            
            
                result = conn.execute(insert(DimPedido)
                                    ,[{"ref_contrato":int(arg["NumeroContrato"]),"ref_produto":int(arg["produtoId"])
                                    ,"ref_cliente":float(arg["idpessoa"]),"valor_total":arg["totalvenda"],"frete_total":arg["totalfrete"]
                                    ,"data_cadastro":arg["dataCadastro"],"ref_venda":float(arg["idvenda"]),"ref_unidade":int(arg["unidadeId"])
                                    ,"margem":arg["margem"],"quantidade":float(arg["quantidade"])
                                    ,"valor_unitario":arg["valorUnitario"],"frete_unitario":arg["valorFrete"]
                                    ,"custounitario":arg["valorUnitarioCusto"],"bit_ambientado":arg["bitambientado"],
                                    "bit_showroom":arg["pedidoShowroom"],"ref_status":int(arg["statusId"]),"ref_endereco":int(arg["enderecoId"])
                                ,"pessoafisica":arg["pessoafisica"],"dataEstimadaEntrega":arg["dataEstimadaEntrega"]
                                ,"data_montagem":arg["dataEstimadaMontagem"]}])                                    
                
              
     

def insert_fato(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
    
        with dwengine.connect() as conn:
            

            result = conn.execute(insert(FatoVenda)
                                ,[{"ref_contrato":arg["NumeroContrato"]
                                ,"bitambientado":arg["bitambientado"],"data_cadastro":arg["dataCadastro"]
                                ,"total_pedido":arg["totalvenda"],"ref_cliente":arg["pessoaId"],"ref_unidade":arg["lojaid"]}])  




def insert_dim_clientes(*args, **kwargs):
    dwengine = mssq_datawharehouse()
    for arg in args:
    
        with dwengine.connect() as conn:
             
            if conn.execute(select(DimCliente.c.ref_cliente).where(DimCliente.c.ref_cliente == arg['id'])).first():
                print("ja existe")

            else:
                result = conn.execute(insert(DimCliente)
                                ,[{"nome_cliente":arg["nomecliente"],"cpf_cnpj":arg["cpfCnpj"]
                                   ,"ref_cliente":arg["id"],"ref_endereco":arg["enderecoId"],
                                   "bitPessoaFisica":arg["pessoafisica"]}])  



def insert_ambientados(*args, **kwargs):
    for arg in args:
        print(arg)
    
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            
            try:
                result = conn.execute(insert(ambientados)
                    ,[{"ref_ambiente":int(arg["idambiente"]),"ref_venda":int(arg["vendaId"]),
                    "valorAmbiente":float(arg["somaambiente"])
                    ,"ref_loja":int(arg["unidadeId"]),"ref_cliente":int(arg["pessoaId"]) }])
                    
            except Exception as e:

                print("errrro")
                print("error", e)


def insert_dim_status(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            result = conn.execute(insert(DimStatus)
                    ,[{"ref_status":int(arg["id"]),"nome_status":arg["nome"]}])
       
        

def insert_notas_fiscais(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:

            if conn.execute(select(DimStatus.c.ref_status).where(DimStatus.c.ref_status == arg['id'])).first():
                print("ja existe")
            else:
                result = conn.execute(insert(DimStatus)
                        ,[{"ref_status":int(arg["id"]),"nome_status":arg["nome"]}])

def insert_fabricante(*args, **kwargs):
    for arg in args:
        print(arg)
     
        engine = mssq_datawharehouse()
        with engine.connect() as conn:

            try:
                result = conn.execute(insert(DimFabricante)
                        ,[{"ref_fabricante":arg["ID"],
                        "nome_fabricante":arg["NomeFabricante"],"planejados":arg["bitPlanejados"]}])
                       
            except Exception as e:
                    print("error", e)


def insert_frequencia_vendas(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            result = conn.execute(insert(DimFrequencia)
                    ,[{"ref_produto":arg['produtoId'],"ref_venda":arg["vendaId"]
                       ,"quantidade":arg["totalitems"],
                       "totalmarca":arg["totalmarca"],"totalambiente":arg["totalambiente"]}])   



def insert_dim_produtos(*args, **kwargs):

    for arg in args:
        
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
                
        
                result = conn.execute(insert(dim_produtos)
                        ,[{"ref_produto":arg["id"],"ref_categoria":arg["categoriaId"], "ref_fabricante":arg["fabricanteId"]
                           ,"nome_produto":str(arg["nome"]).capitalize(),"cod_barras":arg["CodigoBarras"],"marca":str(arg["marca"]).capitalize()}])                      
                                                    

def insert_dim_pessoas(*args, **kwargs):
    for arg in args:
        print(arg)
  
        cidade = str(arg["cidade"]).replace("'","").replace(
            "''","").replace(
            "á","a").replace("ã","a").replace("í","i").strip().capitalize()
       
        try:
            engine = mssq_datawharehouse()
            with engine.connect() as conn:
                if conn.execute(select(DimCliente.c.ref_cliente).where(DimCliente.c.ref_cliente == arg['id'])).first():
                    print("ja existe")
                else:

                    result = conn.execute(insert(DimCliente)
                            ,[{"nome_cliente":str(arg["nomecliente"]).strip().capitalize(),"ref_cliente":arg["id"]
                        ,"bitPessoaFisica":arg["pessoafisica"]
                        ,"ref_endereco":arg["enderecoId"]
                        ,"cidade":cidade,"uf":arg["uf"]
                        ,"cpf_cnpj":str(arg["cpfCnpj"]).strip(),
                        "cidade":arg["cidade"]}])    

        except:
            pass

def insert_custo_produtos(*args, **kwargs):
    for arg in args:
        print(arg)

 
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(DimCustos.c.ref_produto).where(DimCustos.c.ref_produto == arg['produtoId'])).first():
                pass

            else:
                try:
                    result = conn.execute(insert(DimCustos)
                        ,[{"frete":arg["frete"],"royaltie":arg["royalties"],"valor_vendido":arg["precoVenda"]
                           ,"lucro":arg["lucro"],"margem":arg["margem"],"ipi":arg["ipi"]
                           ,"desconto_recomendado":arg["descontoMaximoRecomendado"],"data_alteracao":arg["dataAlteracao"]
                           ,"data_cadastro":arg["datacadastrodw"]
                           ,"comissao_vendedor":arg["comissaoVendedor"],"comissao_arquiteto":arg["comissaoArquiteto"]
                           ,"icms":arg["icms"],"st":arg["st"]
                           ,"custo":arg["custo"],"outros_custos":arg["outroscustos"]
                           ,"ref_produto":arg["produtoId"],"ref_loja":arg["unidadeId"]}] )
                
                except Exception as e:
                    print("error", e)


  
def insert_dim_notas_fiscais(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(dim_notafiscal.c.ref_nota).where(dim_notafiscal.c.ref_nota == arg['numero'])).first():
                pass
            else:

                try:
                    result = conn.execute(insert(dim_notafiscal)
                        ,[{"ref_nota":arg["numero"],"ref_pedido":arg["vendaId"],
                        "ref_cliente":arg["pessoaId"]
                        ,"bitemissao":arg["bitEmitida"],"bitcancelamento":arg["bitErro"],"quantidade_itens":arg["quantidade"]
                        ,"valor_nota":arg["total"],"dataCadastro":arg["dataCadastro"],"dataatualizacaonf":arg["dataAtualizacao"]
                        ,"formapagamento":arg["formaPagamento"]}])
                        
                except Exception as e:

                    print("error", e)



def insert_dim_unidade(*args, **kwargs):

    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(DimLoja.c.ref_loja).where(DimLoja.c.ref_loja == arg['id'])).first():
                pass
            else:


                try:
                    result = conn.execute(insert(DimLoja)
                        ,[{"nome_loja":arg["nome"],"ref_endereco":arg["enderecoId"],"ref_loja":arg["id"]}])
                        
                except Exception as e:

                    print("error", e)




def insert_categorias(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            
                try:
                    result = conn.execute(insert(dim_categorias)
                        ,[{"nome_categoria":arg["nome"],"ref_categoria":int(arg["id"])}])
                        
                except Exception as e:

                    print("error", e)


def insert_cidade(*args, **kwargs):
    for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(DimCidade.c.idendereco).where(DimCidade.c.idendereco == arg['idendereco'])).first():
                pass
            else:

                try:
                    result = conn.execute(insert(DimCidade)
                        ,[{"nome_cidade":arg["cidade"]
                        ,"ref_endereco":arg["idendereco"],"uf":arg["uf"]}])
                        
                except Exception as e:

                    print("error", e)



def insert_marca(*args, **kwargs):
     for arg in args:
        print(arg)
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
                
                    
                try:
                    result = conn.execute(insert(DimMarca)
                            ,[{"ref_fabricante":arg["ID"]
                            ,"marca":arg["slug"]}])
                            
                except Exception as e:

                    print("error", e)



def insert_enderecos(*args, **kwargs):
    for arg in args:
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(DimEndereco.c.ref_endereco).where(DimEndereco.c.ref_endereco == arg['idendereco'])).first():
                pass
            try:
                result = conn.execute(insert(DimEndereco)
                    ,[{"logradouro":arg["logradouro"],"complemento":arg["complemento"]
                       ,"uf":arg["uf"],"ref_endereco":arg["idendereco"] }])
                                               
            except Exception as e:
                print("error", e)
       
    
def insert_dim_status(*args, **kwargs):
    for arg in args:
        print(arg['id'], arg['nome'])
   
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            if conn.execute(select(DimStatus.c.ref_status).where(DimStatus.c.ref_status == arg['id'])).first():
                pass

            try:
                result = conn.execute(insert(DimStatus)
                    ,[{"nome_status":arg['nome'],"ref_status":arg["id"]}])
                                
            except Exception as e:
                print("error", e)


def insert_dim_ambientados(*args, **kwargs) -> None:
    for arg in args:
        print(arg['vendaId'],arg['idambiente'],arg['valorAmbiente'],arg['unidadeId'],arg['numeracao'],arg['pessoaId'])
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            result = conn.execute(insert(DimPlanejados)
                ,[{"ref_pedido":arg["vendaId"],"ref_loja":arg["unidadeId"]
                ,"ref_cliente":arg["pessoaId"],"valor_venda":arg["valorAmbiente"]
                ,"data_cadastro":arg["dataCadastro"] ,"ref_venda_ambiente":arg["idambiente"]}])
                    
                                                    

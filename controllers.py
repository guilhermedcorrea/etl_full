from querys import retorna_venda_item, get_soma_ambiente
from sqlalchemy import text
from config import mssql_get_conn
from itertools import chain
import pandas as pd

def get_ambientados():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        items = get_soma_ambiente()

 
        get_items = conn.execute(items).all()
        dict_tems = [row._asdict() for row in get_items]
        return dict_tems
      

def get_items_pedido():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:
        items = retorna_venda_item()
        get_items = conn.execute(items).all()
        dict_tems = [row._asdict() for row in get_items]
        return dict_tems


def get_pedidos():
    enginemssql = mssql_get_conn()
    with enginemssql.begin() as conn:

        call_procedure = (text(
                """EXEC [dbo].[ETL_Dados_vendas_detalhe]  @data = '2020-01-01'"""))
        exec_procedure = conn.execute(call_procedure).all()
        dict_items = [row._asdict() for row in exec_procedure]
        cont = len(dict_items)
        i = 0
        new_item = get_ambientados()
        lista_pedidos = []
        while i < cont:

            for item in chain(new_item):
                if dict_items[i]['idvenda'] == item['vendaId']:

                    new_dict = {
                        "idvenda":dict_items[i]["idvenda"],
                        "unidade":dict_items[i]["unidade"],
                        "totalDesconto":dict_items[i]["totalDesconto"],
                        "NumeroContrato":dict_items[i]["NumeroContrato"],
                        "enderecoId":dict_items[i]["enderecoId"],
                        "lojaid":dict_items[i]["lojaid"],
                        "cidade":dict_items[i]["cidade"],
                        "uf":dict_items[i]["uf"],
                        "status":dict_items[i]["nome"],
                        "totalvenda":item["totalVenda"],
                        "bitambientado":item["bitambientado"],
                        "totalfrete":dict_items[i]["totalFrete"],
                        "dataCadastro":dict_items[i]["dataCadastro"],
                        "idcliente":dict_items[i]["idcliente"],
                        "statusId":dict_items[i]["statusId"],
                        "pedidoShowroom":dict_items[i]["pedidoShowroom"]
                        }
                  
                
                    lista_pedidos.append(new_dict)

            i +=1
        return lista_pedidos
       
          
def create_dict_orders():
    itenspedido = get_items_pedido()
    pedidossomados = get_pedidos()
    itenspedidos = [{**order} for order in itenspedido]
    cont = len(itenspedidos)
    i = 0
    lista_dicts = []
    while i < cont:
        for item in pedidossomados:
            if itenspedidos[i]['vendaId'] == item['idvenda']:

                new_dict_order = {
                    "idvenda":item["idvenda"],
                        "unidade":item["unidade"],
                        "totalDesconto":item["totalDesconto"],
                        "NumeroContrato":item["NumeroContrato"],
                        "enderecoId":item["enderecoId"],
                        "lojaid":item["lojaid"],
                        "cidade":item["cidade"],
                        "uf":item["uf"],
                        "totalvenda":item["totalvenda"],
                        "bitambientado":item["bitambientado"],
                        "totalfrete":item["totalfrete"],
                        "dataCadastro":item["dataCadastro"],
                        "idcliente":item["idcliente"],
                        "pedidoShowroom":item["pedidoShowroom"]}

               
                new_dict_order.update(itenspedidos[i])
                lista_dicts.append(new_dict_order)
             
               
                
        i +=1
    return lista_dicts


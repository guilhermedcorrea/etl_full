

from sqlalchemy import Table
from sqlalchemy.orm import declarative_base
from sqlalchemy import Table, MetaData, Float, Integer,ForeignKey,DateTime, Boolean, String, Column
from datetime import datetime


from config import mssq_datawharehouse

engine = mssq_datawharehouse()
metadata = MetaData()
metadata_obj = MetaData(schema="comercial")



#[comercial].[dim_pedido]
DimPedido = Table(
"dim_pedido",
metadata,
Column('cod_venda',Integer, primary_key=True),
Column('cod_loja',Integer),
Column('cod_cliente',Integer),
Column('cod_status',Integer),
Column('ref_venda',Integer),
Column('ref_produto',Integer),
Column('ref_cliente',Integer),
Column('valor_total',Float),
Column('frete_total',Float),
Column('data_cadastro',DateTime),
Column('ref_pedido',Integer),
Column('ref_unidade',Integer),
Column('margem',Float),
Column('quantidade',Integer),
Column('planejado',Boolean),
Column('valor_unitario',Float),
Column('frete_unitario',Float),
Column('custounitario', Float)
,schema="comercial",extend_existing=True)




DimCidade = Table(
"dim_cidade",
metadata,
Column('cod_cidade',Integer,primary_key=True),
Column('cod_estado',Integer),
Column('nome_cidade',String),
Column('data_cadastro',DateTime)
,schema="comercial")


DimCliente = Table(
"dim_cliente",
metadata,
Column('cod_cliente',Integer,primary_key=True),
Column('cod_endereco',Integer),
Column('nome_cliente',String),
Column('cpf_cnpj',String),
Column('ref_cliente',Integer),
Column('ref_endereco',Integer),
Column('data_ultimo_pedido',DateTime),
Column('valor_pedido',Float),
Column('bitPessoaFisica',Boolean),
Column('bitativo',Boolean),
Column('data_cadastro',DateTime),
Column('data_alteracao',DateTime)
,schema="comercial",extend_existing=True)


DimCustos = Table(
"dim_custos",
metadata,
Column('cod_custos',Integer,primary_key=True),
Column('cod_loja',Integer),
Column('cod_estado',Integer),
Column('cod_endereco',Integer),
Column('cod_produto',Integer),
Column('custo_total',Integer),
Column('frete',Float),
Column('royaltie',Float),
Column('valor_vendido',Float),
Column('desconto',Float),
Column('lucro',Float),
Column('margem',Float),
Column('valor_recomendado',Float),
Column('desconto_recomendado',Float),
Column('data_alteracao',DateTime),
Column('data_cadastro',DateTime),
Column('comissao_vendedor',Float),
Column('comissao_arquiteto',Float),
Column('icms',Float),
Column('st',Float),
Column('custo', Float),
Column('outros_custos',Float),
Column('ref_produto', Integer),
Column('ref_loja',Integer)

,schema="comercial",extend_existing=True)
   

   
DimEndereco = Table(
"dim_endereco",
metadata,
Column('cod_endereco',Integer,primary_key=True),
Column('cod_estado',Integer),
Column('cod_cidade',Integer),
Column('logradouro',String),
Column('complemento',String),
Column('uf',String),
Column('cep',String),
Column('numero',String),
Column('ref_endereco',Integer)
,schema="comercial",extend_existing=True)


DimEndereco = Table(
"dim_endereco",
metadata,
Column('cod_estado',Integer,primary_key=True),
Column('nome_estado',String),
Column('uf',String),
Column('data_cadastro',DateTime)
,schema="comercial",extend_existing=True)


DimLoja = Table(
"dim_loja",
metadata,
Column('cod_loja',Integer,primary_key=True),
Column('nome_loja',String),
Column('cod_endereco',String),
Column('ref_endereco',Integer),
Column('bitativo',Boolean),
Column('data_alteracao',DateTime),
Column('data_cadastro',DateTime)
,schema="comercial",extend_existing=True)


DimMarca = Table(
"dim_marca",
metadata,
Column('cod_marca',Integer,primary_key=True),
Column('cod_fabricante',Integer),
Column('ticket_medido',Float),
Column('ref_fabricante',Integer),
Column('representatividade',Float),
Column('categoria_principal',Float),
Column('ref_marca',Integer)
,schema="comercial",extend_existing=True)


dim_notafiscal = Table(
"dim_notafiscal",
metadata,
Column('cod_nota',Integer,primary_key=True),
Column('cod_pedido',Integer),
Column('cod_cliente',Integer),
Column('cod_status',Integer),
Column('cod_endereco',Integer),
Column('ref_nota',Integer),
Column('ref_pedido',Integer),
Column('ref_cliente',Integer),
Column('data_emissao',DateTime),
Column('bitemissao',Boolean),
Column('bitcancelamento',Boolean),
Column('quantidade_itens',Integer),
Column('valor_nota',Float)
,schema="comercial",extend_existing=True)


DimProduto = Table(
"dim_produto",
metadata,
Column('cod_produto',Integer,primary_key=True),
Column('cod_loja',Integer),
Column('cod_marca',Integer),
Column('cod_fabricante',Integer),
Column('ref_produto',Integer),
Column('ref_loja',Integer),
Column('preco_venda',Float),
Column('custo',Float),
Column('desconto_aplicado',Float),
Column('frete',Float),
Column('comissao_vendedor',Float),
Column('comissao_arquiteto',Float),
Column('preco_total',Float),
Column('nome_produto',String),
Column('codigo_barras',String),
Column('data_alteracao',DateTime),
Column('bitativo',Boolean),
Column('bitshowroom',Boolean),
Column('ref_categoria',Integer)
,schema="comercial",extend_existing=True)


DimFabricante = Table(
    "dim_fabricante",
    metadata,
    Column('cod_fabricante', Integer, primary_key=True),
    Column('cod_endereco',Integer),
    Column('cod_marca',Integer),
    Column('ref_fabricante',Integer),
    Column('nome_fabricante',String),
    Column('pedido_minimo',Float),
    Column('representatividade',Float),
    Column('data_ultimo_pedido',DateTime),
    Column('prazo_medio',Integer),
    Column('bitativo',Boolean),
    Column('planejados', Boolean)
    ,schema="comercial",extend_existing=True)
    

DimStatus = Table(
"dim_status",
metadata,
Column('cod_status',Integer,primary_key=True),
Column('nome_status',Integer),
Column('data_cadastro',Integer),
Column('data_alteracao',Integer),
Column('ref_status',Integer)
,schema="comercial",extend_existing=True)


FatoVenda = Table(
"fato_venda",
metadata,
Column('cod_venda',Integer,primary_key=True),
Column('cod_pedido',Integer),
Column('cod_status',Integer),
Column('cod_cliente',Integer),
Column('cod_loja',Integer),
Column('cod_endereco',Integer),
Column('cod_nota',Integer),
Column('cod_transportadora',Integer),
Column('cod_cidade',Integer),
Column('cod_estado',Integer),
Column('ref_nota',Integer),
Column('ref_pedido',Integer),
Column('custo_total',Float),
Column('frete_total',Float),
Column('taxa_total',Float),
Column('lucro_total',Float),
Column('margem',Float),
Column('total_pedido',Float),
Column('nome_status',String),
Column('quantidade_itens',Integer),
Column('quantidade_marcas',Integer),
Column('bitshowroom',Boolean),
Column('planejado',Boolean),
Column('bitentregue',Boolean),
Column('bitmontado',Boolean),
Column('bitvendajuridica',Boolean),
Column('bitatrasobit',Boolean),
Column('bitavaria',Boolean),
Column('data_cadastro',DateTime)
,schema="comercial",extend_existing=True)


  

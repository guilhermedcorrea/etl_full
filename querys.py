from sqlalchemy import text
from typing import Any


def get_unidades():
    call = (text("""
        SELECT distinct id,nome,enderecoId,endereçoEntregaId
        FROM [myboxmarcenaria].[dbo].[Unidade] """))
    return call
    
    
def get_soma_ambiente():
    call = (text("""
        
        select * ,SUM(V.totalAmbiente + v.totalItem) as totalVenda
        from (
        select distinct venda.id as vendaId
        ,unidadeId,
                    
                    case 
                        when ambiente.valorAmbiente is not null then 1
                        when ambiente.valorAmbiente is null then 0
                        else 0
                        end 'bitambientado'
        ,case
        when
        SUM(ambiente.valorAmbiente) OVER (PARTITION BY ambiente.vendaId) is null
        then
        0
        else
        SUM(ambiente.valorAmbiente) OVER (PARTITION BY ambiente.vendaId)
        end
        AS totalAmbiente
        ,case
        when
        SUM((vitem.valorUnitario * vitem.quantidade)) OVER (PARTITION BY vitem.vendaId) is null
        then
        0
        else
        SUM((vitem.valorUnitario * vitem.quantidade)) OVER (PARTITION BY vitem.vendaId)
        end
        AS totalItem
        from [dbo].[Venda] as venda
        left join [dbo].[VendaItem] as vitem on vitem.vendaId = venda.id
        left join [dbo].[VendaAmbiente] as ambiente on ambiente.vendaId = venda.id
        left join [dbo].Unidade as unidade on unidade.id = venda.unidadeId
        where unidade.id not in(1, 85, 89, 127)
        and unidade.excluido = 0
        and venda.statusId > 2
        and venda.numeracao <> 0
        and venda.excluido = 0
        ) V

        group by V.vendaId
        ,V.unidadeId
        ,V.totalAmbiente
        ,V.totalItem
                ,V.bitambientado"""))

    return call
    

def retorna_venda_item():

    call = (text("""
            SELECT DISTINCT CONVERT(INTEGER,vitem.[vendaId]) AS vendaId,CONVERT(INTEGER,vitem.[produtoId]) AS produtoId,
            CONVERT(INTEGER,vitem.[quantidade])AS quantidade,CONVERT(FLOAT,vitem.[valorUnitario]) AS valorUnitario,CONVERT(INTEGER,vitem.[total]) AS total
            ,CONVERT(DATETIME,vitem.[dataCadastro])AS dataCadastro,CONVERT(FLOAT,vitem.[valorUnitarioCusto]) AS valorUnitarioCusto,

			CASE
			WHEN CONVERT(FLOAT,vitem.[valorFrete]) IS NULL THEN 0
			WHEN CONVERT(FLOAT,vitem.[valorFrete]) IS NOT NULL THEN CONVERT(FLOAT,vitem.[valorFrete])
			ELSE 0
			END valorFrete,

			CASE
			WHEN CONVERT(INTEGER,venda.numeracao) IS NULL THEN 0
			WHEN CONVERT(INTEGER,venda.numeracao) IS NOT NULL THEN CONVERT(INTEGER,venda.numeracao)
			ELSE 0
			END numeracao,

			CASE
			WHEN ROUND(((CONVERT(FLOAT,vitem.valorUnitario) / NULLIF(CONVERT(FLOAT,vitem.valorUnitarioCusto),0) -1) *100 ),2) IS NULL THEN 0
			WHEN ROUND(((CONVERT(FLOAT,vitem.valorUnitario) / NULLIF(CONVERT(FLOAT,vitem.valorUnitarioCusto),0) -1) *100 ),2) IS NOT NULL THEN ROUND(((CONVERT(FLOAT,vitem.valorUnitario) / NULLIF(CONVERT(FLOAT,vitem.valorUnitarioCusto),0) -1) *100 ),2)
			ELSE 0
			END margem,

			CONVERT(DATETIME,venda.dataAlteracao) AS dataAlteracao
            ,CONVERT(DATETIME,venda.dataCadastro)AS dataCadastro
            ,CONVERT(DATETIME,venda.dataEstimadaEntrega) AS dataEstimadaEntrega,CONVERT(DATETIME,venda.dataEstimadaMontagem) AS dataEstimadaMontagem
            ,CONVERT(INTEGER,venda.unidadeId) AS unidadeId,CONVERT(INTEGER,venda.statusId) AS statusId,CONVERT(FLOAT,venda.totalAdicional) AS talAdicional
            ,CONVERT(FLOAT,venda.totalAdicional),CONVERT(FLOAT,venda.totalFrete) AS totalFrete,CONVERT(FLOAT,venda.totalDesconto) AS totalDesconto,
            CONVERT(INTEGER,vitem.produtoId) AS produtoId,CONVERT(FLOAT,vitem.valorUnitario) AS vitem
            ,CONVERT(FLOAT,vitem.valorUnitarioCusto) AS valorUnitarioCusto,CONVERT(FLOAT,vitem.total) AS total
            ,CONVERT(INTEGER,venda.statusId) AS statusId,statusvenda.nome as statuspedido,pessoa.id as idpessoa
            ,CONVERT(INTEGER,pessoa.enderecoId) AS enderecoId,pessoa.nomeCompletoRazaoSocial,pessoa.pessoafisica,endereco.bairro
            ,endereco.cep,endereco.cidade,endereco.complemento,endereco.uf,endereco.numero,unidade.nome as nomeunidade
            ,CONVERT(DATETIME, unidade.dataAbertura) AS dataAbertura,CONVERT(DATETIME,unidade.dataContrato) AS dataContrato
            
            FROM  [myboxmarcenaria].[dbo].[VendaItem] vitem
            left join [dbo].[Venda] venda
            ON venda.id = vitem.[vendaId]
            left join [myboxmarcenaria].[dbo].[VendaStatus] as statusvenda
            ON statusvenda.id = venda.statusId
            left join [myboxmarcenaria].[dbo].[Pessoa] as pessoa
            ON pessoa.id = venda.pessoaId
            left join [myboxmarcenaria].[dbo].[Endereco] as endereco
            ON endereco.id = pessoa.enderecoId
            left join [dbo].[Unidade] as unidade
            ON unidade.id = venda.unidadeId

            WHERE vitem.[vendaId] IN (SELECT DISTINCT vendas.id
            from [dbo].[Venda] vendas
            left join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = VENDAS.unidadeId
                left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv
                on statuspv.id = VENDAS.statusId
                left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv
                ON statusv.id = VENDAS.statusId
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0)"""))
    

    return call
                    


def get_clientes():
    call = (text("""
        SELECT DISTINCT pessoa.nomeCompletoRazaoSocial as nomecliente,pessoa.id,pessoa.cpfCnpj, pessoa.enderecoId,endereco.uf
        ,endereco.cep,endereco.cidade
        ,endereco.logradouro,endereco.complemento,endereco.uf,
        pessoa.pessoafisica  from [myboxmarcenaria].[dbo].[Pessoa] as pessoa
        inner join [myboxmarcenaria].[dbo].[Endereco] as endereco
        on endereco.id = pessoa.enderecoId
        WHERE pessoa.id in
        (SELECT DISTINCT vendas.pessoaId
                from [dbo].[Venda] vendas
                left join [myboxmarcenaria].[dbo].[Unidade] AS LOJA ON LOJA.id = VENDAS.unidadeId
                left join [myboxmarcenaria].[dbo].[VendaStatus] as statuspv
                on statuspv.id = VENDAS.statusId
                left join [myboxmarcenaria].[dbo].[VendaComunicacaoStatus] as statusv
                        ON statusv.id = VENDAS.statusId
                WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                and LOJA.excluido = 0
                and VENDAS.statusId > 2
                and VENDAS.excluido = 0
                and VENDAS.numeracao <> 0)"""))
    return call
    
    

def get_products_prices():
    call = (text("""SELECT CONVERT(INT,preco.id) AS idpreco,CONVERT(INT,preco.produtoId) AS produtoId
            ,CONVERT(int,preco.unidadeId) AS unidadeId,CONVERT(float,preco.precoVenda) AS precoVenda,preco.dataAlteracao
            ,CONVERT(DATETIME,preco.dataAlteracao) AS dataAlteracao
            ,CONVERT(float,preco.custo) AS custo, CONVERT(float,preco.descontoMaximoRecomendado) AS descontoMaximoRecomendado
            ,CONVERT(float,preco.icms) AS icms,CONVERT(float,preco.st) st, CONVERT(float,preco.ipi) AS ipi,CONVERT(float,preco.royalties) AS royalties
            ,CONVERT(float,preco.taxaFinanceira) AS   taxaFinanceira    
            ,preco.frete ,preco.comissaoVendedor ,preco.comissaoArquiteto    
            ,preco.precoCompletoSemComissao, CONVERT(DATETIME, GETDATE()) as datacadastrodw, 
            ROUND(CONVERT(float,(CONVERT(float,preco.precoVenda)- (CONVERT(float,preco.custo)))),2) as lucro,
            ROUND(CONVERT(float,(CONVERT(float,preco.precoVenda)/ NULLIF(CONVERT(float,preco.custo),0)-1) * 100),2) as margem,
            CONVERT(float,CONVERT(float,preco.icms) +  CONVERT(float,preco.icms) + CONVERT(float,preco.st)
                            + CONVERT(float,preco.ipi) + CONVERT(float,preco.royalties)+ CONVERT(float,preco.taxaFinanceira)) AS outroscustos
                                
            FROM myboxmarcenaria.dbo.ProdutoPreco as preco
            where  preco.produtoId in (
            SELECT vitem.produtoId
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem
            on vitem.vendaId = VENDAS.id
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0 and LOJA.id = preco.unidadeId and preco.produtoId = vitem.produtoId)"""))

    return call

def get_status():
    call = (text("""SELECT [id],[nome],[prazoStatus],[ordemStatus],[bitSite] 
        FROM [myboxmarcenaria].[dbo].[VendaComunicacaoStatus]"""))
    return call
    
    

def get_fabricante():

    call = (text("""
            select distinct fabrica.ID,fabrica.Cidade
            ,fabrica.NomeFabricante,fabrica.bitPlanejados,fabrica.slug
            FROM [dbo].[Fabricante] as fabrica"""))


            

    return call


def get_enderecos_client():
    call = (text("""
                select distinct venda.pessoaId, pessoa.id as idcliente, endereco.id as idendereco, endereco.bairro
                ,endereco.uf,endereco.logradouro,endereco.cidade,endereco.complemento,pessoa.pessoafisica,pessoa.ativo
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
                WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                and LOJA.excluido = 0
                and VENDAS.statusId > 2
                and VENDAS.excluido = 0
                and VENDAS.numeracao <> 0)"""))

    return call


def get_produtos_cadastros():
    call = (text("""
            SELECT distinct produto.[id],produto.[nome] ,produto.[categoriaId] ,produto.[unidadeId]
                ,produto.[marca],produto.[fabricanteId],produto.[CodigoBarras]
                ,categoria.nome as nomecategoria,produto.codigoFabricante
                FROM [myboxmarcenaria].[dbo].[Produto] as produto
                inner join [myboxmarcenaria].[dbo].[Categoria] as categoria
                on categoria.id = produto.categoriaId
                where produto.id in 
                (SELECT vitem.produtoId
                from venda VENDAS
                left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                and LOJA.excluido = 0
                and VENDAS.statusId > 2
                and VENDAS.excluido = 0
                and VENDAS.numeracao <> 0)"""))

    return call


def get_categorias():
    call = (text("""SELECT [id] ,[nome],[impostoMedio] ,[margemMedia]
      ,[habilitaSite],[categoriaPaiId] ,[nivel]
        FROM [myboxmarcenaria].[dbo].[Categoria]"""))
    return call

def get_frequencia_venda():
    call = (text("""
    
            select DISTINCT vitem.produtoId,vitem.vendaId,vitem.quantidade,vitem.valorFrete
            ,vitem.valorUnitarioCusto,vitem.valorUnitarioCusto
            ,fabricante.NomeFabricante,produto.marca,
            COUNT(vitem.produtoId) OVER(PARTITION BY vitem.vendaId) as totalitems,
            COUNT(produto.marca) OVER(PARTITION BY vitem.vendaId) as totalmarca,
            COUNT(vambiente.vendaId) OVER(PARTITION BY vitem.vendaId) as totalambiente
            from  [dbo].[VendaItem] as vitem
            inner join [myboxmarcenaria].[dbo].[Produto] as produto
            on produto.id = vitem.produtoId
            inner join [myboxmarcenaria].[dbo].[Fabricante] as fabricante
            on fabricante.id = produto.fabricanteId
            inner join [myboxmarcenaria].[dbo].[VendaAmbiente]as vambiente
            on vambiente.vendaId = vitem.vendaId

            where produto.id in (
            SELECT vitem.produtoId
            from venda VENDAS
            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
            and LOJA.excluido = 0
            and VENDAS.statusId > 2
            and VENDAS.excluido = 0
            and VENDAS.numeracao <> 0) """))
    
    return call
                
    
   
def get_ambientados():
    call = (text("""select distinct 
            vitem.vendaId
            ,CONVERT(FLOAT,vitem.valorUnitario) AS valorUnitario
            ,CONVERT(FLOAT,vitem.valorUnitarioCusto) AS valorUnitarioCusto
            ,CONVERT(FLOAT,ambiente.valorAmbiente) AS valorAmbiente
            ,CONVERT(FLOAT,vitem.valorFrete) AS valorFrete
            ,CONVERT(datetime,vitem.dataCadastro) as dataCadastro
            ,CONVERT(INTEGER,venda.pessoaId) AS pessoaId
            ,CONVERT(INTEGER,venda.unidadeId) AS unidadeId
            ,CONVERT(INTEGER,venda.numeracao) AS numeracao
            ,venda.prazoVenda
            ,venda.pedidoShowroom
            ,CONVERT(INTEGER,venda.numeracao) AS numeracao
            ,ambiente.id as idambiente
            ,CONVERT(FLOAT,sum(CONVERT(FLOAT,(vitem.valorUnitario*vitem.quantidade)))) as total
            ,CONVERT(FLOAT,sum(ambiente.valorAmbiente)) AS somaambiente
            from [myboxmarcenaria].[dbo].[VendaItem] as vitem
            left join [myboxmarcenaria].[dbo].[VendaAmbiente] ambiente
            on ambiente.vendaId = vitem.vendaId
            left join [myboxmarcenaria].[dbo].[Venda] as venda
            on venda.id = vitem.vendaId
            where ambiente.[vendaId] in (
                SELECT VENDAS.id
                from venda VENDAS
                            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                            and LOJA.excluido = 0
                            and VENDAS.statusId > 2
                            and VENDAS.excluido = 0
                            and VENDAS.numeracao <> 0)
                        GROUP BY vitem.vendaId,vitem.valorUnitario,vitem.valorUnitarioCusto, ambiente.valorAmbiente,
                        vitem.valorFrete,vitem.dataCadastro,venda.pessoaId,venda.unidadeId,venda.numeracao,
                        venda.prazoVenda,venda.pedidoShowroom,ambiente.id
            """))
    return call


def get_notas():
    call = (text("""
    
        select DISTINCT nf.bitEmitida,nf.bitErro,nf.formaPagamento,nf.numero,venda.pessoaId
        ,SUM(nfproduto.quantidade) AS quantidade,nf.bitEmitida,nf.bitErro,nf.formaPagamento
        ,SUM((valorUnitario * quantidade)) total,nf.vendaId,venda.unidadeId,nf.dataCadastro,nf.dataAtualizacao
        from [dbo].[NotaFiscalProduto] as nfproduto
        inner join [dbo].[NotaFiscal] as nf
        inner join [dbo].[Venda] as venda
        on venda.id = nf.vendaId
        on nf.id = notaFiscalId
        where exists(
        SELECT vitem.vendaId
        from venda VENDAS
                    left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                    left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                    left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                    WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2020-01-01'
                    and LOJA.excluido = 0
                    and VENDAS.statusId > 2
                    and VENDAS.excluido = 0
                    and VENDAS.numeracao <> 0)

        GROUP BY nf.bitEmitida,nf.bitErro,nf.formaPagamento,nf.numero
        ,nfproduto.quantidade,nf.bitEmitida,nf.bitErro,nf.formaPagamento,nf.vendaId,venda.pessoaId,venda.unidadeId
        ,nf.dataCadastro,nf.dataAtualizacao"""))
        
    return call            
                
                

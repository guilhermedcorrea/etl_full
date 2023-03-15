##Execução do projeto
<br>criar venv: py -3 -m venv venv<br/>
<br>ativar: venv\Scripts\activate<br/>
<br>instalar dependencias: pip freeze > requirements.txt<br/>

##Recursos
<br>-Python<br/>
<br>-SQL<br/>

##Factory

```Python
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


class DatabaseFactory:
    @staticmethod
    def get_function(tipo: str) -> Tables:
        if tipo == 'Order':
            return Orders()
            

if __name__=="__main__":
    
    def call_task(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_timestamp = time.time()
            print('LOG: Running job "%s"' % func.__name__)
            
            result = func(*args, **kwargs)
            if func.__name__ == 'task1':
                call_func = DatabaseFactory.get_function('Order')
                call_func.get_values_dabatases()
                call_func.pedidos_itens()

    
@call_task
def task_call_orders():
    return 'Order'
    
```

<br>A classe Tables serve de molde para as demais, como ela tem um 'ABC' abstract method e as demais classes herdam dela, toda classe criada abaixo obrigatoriamente precisa ter uma função def get_values_dabatases(self) que sao usadas para executar as querys referentes a database mybox. <br/>

<br>A classe DatabaseFactory, é resonsavel por chamar os metodos executados. As funções são chamadas atraves de um wrapper  call_task que as funções abaixo referentes ao scheduler recebem um decorador @call_task, assim que da o horario de execução a função é executada o wrapper recebe e retorna para a  DatabaseFactory que verifica a chamada a qual classe se refere fazendo a execução da mesma.<br/>


##Querys

``` Python
def mssql_get_conn():

    connection_url = URL.create(
            "mssql+pyodbc",
            username=f"{myboxuser_mssql}",
            password=f"{myboxpassword_mssql}",
            host=f"{myboxhost_mssql}",
            database=f"{myboxdatabase_mssql}",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "autocommit": "True",
        },
        )
      
    engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT", future=True,fast_executemany=True)
    return engine


def mssq_datawharehouse():

    connection_url = URL.create(
            "mssql+pyodbc",
            username=f"{user_mssql}",
            password=f"{password_mssql}",
            host=f"{host_mssql}",
            database=f"{database_mssql}",
            query={
                "driver": "ODBC Driver 17 for SQL Server",
                "autocommit": "True",
        }
        )
      
    engine = create_engine(connection_url).execution_options(
    isolation_level="AUTOCOMMIT", future=True,fast_executemany=True)
    return engine



```
<br>A conexao da aplicação com as tabelad dp DW e da Mybx sao feitas atraves de uma função que usa o SQLalchemy. as credencias sao passadas atraves de um arquivo .env que é chamado usando o os.getenv('serverhost') e repassado para dentro da função. Qualquer consulta ou ação envolvendo banco de dados é preciso fazer o import e a chamada da função correspondente a database especifica.<br/>

<br>Todas as ações com banco de dados é preciso abrir e fechar a conexao usando o metodo engine<br/>

``` Python
  engine = mssq_datawharehouse()
        with engine.connect() as conn:

```

<br>As querys estao no arquivo querys.py, quase todas as consultas sao feitas baseadas em cima do select<br/>

``` SQL
  SELECT VENDAS.id
                from venda VENDAS
                            left join myboxmarcenaria.dbo.Unidade AS LOJA ON LOJA.id = VENDAS.unidadeId
                            left join dbo.VendaItem as vitem on vitem.vendaId = VENDAS.id
                            left join [dbo].[Pessoa] as pessoa on pessoa.id = VENDAS.pessoaId
                            WHERE LOJA.id not in(1, 85, 89, 127) AND VENDAS.dataCadastro > '2022-01-01'
                            and LOJA.excluido = 0
                            and VENDAS.statusId > 2
                            and VENDAS.excluido = 0
                            and VENDAS.numeracao <> 0)
                        GROUP BY vitem.vendaId,vitem.valorUnitario,vitem.valorUnitarioCusto, ambiente.valorAmbiente,
                        vitem.valorFrete,vitem.dataCadastro,venda.pessoaId,venda.unidadeId,venda.numeracao,
                        venda.prazoVenda,venda.pedidoShowroom,ambiente.id

```


<br>As consultas estao dentro de funções e retorna atraves de um return sendo devolvidas para dentro do metodo da classe da Factory a qual esta sendo chamada<br/>


``` Python

class Ambientados(Tables):
    def get_values_dabatases(self) -> None:
        from querys import get_ambientados
        enginemssql = myboxenginesql()
        with enginemssql.begin() as conn:
            
            item = get_ambientados()
            get_items = conn.execute(item).all()
            dict_tems = [row._asdict() for row in get_items]
           
            insert_ambientados(*dict_tems)

```


<br>Atraves do import o select é chamado para dentro do metodo execute abaixo o listcomp intera no retorno da execução e transforma o retorno do select em um dicionario Python, que é passado para a função responsavel por fazer o insert das informações dentro das tabelas do DW<br/>



``` Python


def insert_fabricante(*args, **kwargs):
    for arg in args:
        print(arg)
     
        engine = mssq_datawharehouse()
        with engine.connect() as conn:
            try:
                result = conn.execute(insert(DimFabricante)
                    ,[{"cod_marca":arg["ID"],
                    "nome_fabricante":arg["NomeFabricante"],"planejados":arg["bitPlanejados"]
                    ,"total_produtos":arg["totalquantidade"]
                    ,"total_produtos_venda":arg["totalvendidofabrica"],"total_custo_venda":arg["totalcusto"]
                    ,"quantidade_skus":arg["totalproduto"] }])
            except Exception as e:
                print("errrro")
              

```


<br>Toda a ação é orquestarda atraves do Scheduler wrapper e da classe responsavel por retornar as funções chamadas da Factory<br/>
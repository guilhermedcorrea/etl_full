import schedule
import time
from functools import wraps
from app import (Tables, Orders, Ambientados, Status,
                  CustoProdutos,Clientes, Fabricante, Produtos, NotasFiscais
                  , FrequenciaVenda,Categorias,Marcas,Cidade,Lojas,Categorias)



from typing import Literal, Any
class DatabaseFactory:
    @staticmethod
    def get_function(tipo: str) -> Tables:
        if tipo == 'Order':
            return Orders()
        
        if tipo == 'Ambientado':
            return Ambientados()
        
        if tipo == "Status":
            return Status()
        
        if tipo == 'CustoProdutos':
            return CustoProdutos()
        
        if tipo =='Clientes':
            return Clientes()

        if tipo == 'Produtos':
            return Produtos()

        if tipo == "Fabricante":
            return Fabricante()
        
        if tipo == "Categorias":
            return Categorias()
        
        if tipo == "Notas":
            return NotasFiscais()
        
        if tipo == "Frequencia":
            return FrequenciaVenda()
        
        if tipo == "Marcas":
            return Marcas()
        
        if tipo == 'Cidade':
            return Cidade()
        
        if tipo == 'Lojas':
            return Lojas()
        
        if tipo == 'Categorias':
            return Categorias()
        

if __name__=="__main__":

    def call_task(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_timestamp = time.time()
            print('LOG: Running job "%s"' % func.__name__)
            
            result = func(*args, **kwargs)
            if func.__name__ == 'task_call_orders':
                call_func = DatabaseFactory.get_function('Order')
                call_func.get_values_dabatases()
            

                
            if func.__name__ == 'task_call_produtos':
                call_func = DatabaseFactory.get_function('Produtos')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_categorias':
                call_func = DatabaseFactory.get_function('Categorias')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_notas':
                call_func = DatabaseFactory.get_function('Notas')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_frequenciavenda':
                call_func = DatabaseFactory.get_function('Frequencia')
                call_func.get_values_dabatases()

            
            if func.__name__=='task_call_marcas':
                call_func = DatabaseFactory.get_function('Marcas')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_cidade':
                call_func = DatabaseFactory.get_function('Cidade')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_custo_produtos':
                call_func = DatabaseFactory.get_function('CustoProdutos')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_clientes':
                call_func = DatabaseFactory.get_function('Clientes')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_fabricante':
                call_func = DatabaseFactory.get_function('Fabricante')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_lojas':
                call_func = DatabaseFactory.get_function('Lojas')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_categorias':
                call_func = DatabaseFactory.get_function('Categorias')
                call_func.get_values_dabatases()


       

            ''' 
                
            if func.__name__=='task_call_ambientados':
                call_func = DatabaseFactory.get_function('Status')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_status':
                call_func = DatabaseFactory.get_function('CustoProdutos')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_custo_produtos':
                call_func = DatabaseFactory.get_function('Clientes')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_clientes':
                call_func = DatabaseFactory.get_function('Fabricante')
                call_func.get_values_dabatases()


            if func.__name__=='task_call_fabricante':
                call_func = DatabaseFactory.get_function('Produtos')
                call_func.get_values_dabatases()

            if func.__name__=='task_call_produtos':
                call_func = DatabaseFactory.get_function('Produtos')
                call_func.get_values_dabatases()

            '''
            return result
        
        return wrapper


    @call_task
    def task_call_orders(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Order']:
        return 'Order'


    @call_task
    def task_call_ambientados(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Ambientados']:
        return 'Ambientados'
    

    @call_task
    def task_call_status(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Status']:
        return 'Status'
    
    @call_task
    def task_call_custo_produtos(*args: tuple, **kwargs: dict[str, Any]) -> Literal['CustoProdutos']:
        return 'CustoProdutos'
    

    @call_task
    def task_call_clientes(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Clientes']:
        return 'Clientes'

    @call_task
    def task_call_fabricante(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Fabricante']:
        return 'Fabricante'
    

    @call_task
    def task_call_produtos(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Produtos']:
        return 'Produtos'
   
    @call_task
    def task_call_categorias(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Categorias']:
        return 'Categorias'
    
    @call_task
    def task_call_notas(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Notas']:
        return 'Notas'
    
    @call_task
    def task_call_frequenciavenda(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Frequencia']:
        return 'Frequencia'
    

    @call_task
    def task_call_marcas(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Marcas']:
        return 'Marcas'

    @call_task
    def task_call_cidade(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Cidade']:
        return 'Cidade'
    
    @call_task
    def task_call_lojas(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Lojas']:
        return 'Lojas'
    
    def task_call_categorias(*args: tuple, **kwargs: dict[str, Any]) -> Literal['Categorias']:
        return 'Categorias'
    



task_call_categorias()

'''
    task1 = schedule.every().day.at('10:30').do(task_call_orders)

    task2 = schedule.every().day.at('11:30').do(task_call_ambientados)

    task3 = schedule.every().day.at('11:30').do(task_call_status)

    task4 = schedule.every().day.at('11:30').do(task_call_custo_produtos)  

    task5 = schedule.every().day.at('11:30').do(task_call_clientes)  

    task6 = schedule.every().day.at('12:30').do(task_call_fabricante)

    task7 = schedule.every().day.at('12:30').do(task_call_produtos)



    while True:
        schedule.run_pending()
        time.sleep(1)

'''
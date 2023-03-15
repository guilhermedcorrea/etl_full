from config import mssq_datawharehouse
from my_box_comercial_dw import FatoVenda
from sqlalchemy import select, update
import pandas as pd

def get_orders():
    engine = mssq_datawharehouse()

    with engine.connect() as conn:
        
        get_orders = conn.execute(select(FatoVenda.c.custo_total, FatoVenda.c.total_pedido,FatoVenda.c.ref_venda)).all()
        dict_tems = [row._asdict() for row in get_orders]

        df = pd.DataFrame(dict_tems)
        df['lucro'] = df['total_pedido'] - df['custo_total']

        new_dict = df.to_dict('records')
        for new in new_dict:
            print(new)
          
            if conn.execute(select(FatoVenda.c.custo_total,
                                    FatoVenda.c.total_pedido).where(FatoVenda.c.ref_venda == new['ref_venda'])).first():
                

                stmt = (update(FatoVenda).where(FatoVenda.c.ref_venda == new['ref_venda'])).values(lucro_total=new['lucro'])
                conn.execute(stmt)
                    
                print(new)


def update_marge():
    engine = mssq_datawharehouse()

    with engine.connect() as conn:
        
        get_orders = conn.execute(select(FatoVenda.c.custo_total, FatoVenda.c.total_pedido,FatoVenda.c.ref_venda,FatoVenda.c.margem)).all()
        dict_tems = [row._asdict() for row in get_orders]

        df = pd.DataFrame(dict_tems)

        df['Margem'] = round((df['total_pedido'] / df['custo_total']-1) * 100,2)
        df['Margem'] = df['Margem'].astype(float)

        new_dict = df.to_dict('records')
        for new in new_dict:
            try:
                if conn.execute(select(FatoVenda.c.ref_venda).where(
                    FatoVenda.c.ref_venda == new['ref_venda'])).first():
                    if isinstance(new['Margem'], float):
            
                        stmt = (update(FatoVenda).where(FatoVenda.c.ref_venda == new['ref_venda'])).values(margem=new['Margem'])
                        conn.execute(stmt)
                            
            except:
                pass


          
      
update_marge()

                    
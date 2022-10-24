# Executa query sql
import conecta_db as cn

def executa_query(sql, hostx, databasex, userx, passwordx):
  con = cn.conecta_db(hostx, 
                      databasex,
                      userx, 
                      passwordx)
  cur = con.cursor()
  cur.execute(sql)
  con.commit()
  con.close()
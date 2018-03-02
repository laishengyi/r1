import pymysql
import sys


class TransferMonery(object):

    def __init__(self,conn):
        self.conn = conn

    def check_acct_available(self,acctid):
        cursor = self.conn.cursor()
        try:
            sql = "select * from account where acctid=%s"%{acctid}
            cursor.execute(sql)
            print('check_acct_available:'+sql)
            rs = cursor.fectchall()
            if len(rs) != 1:
                raise (Exception('帳號%s不存在'%(acctid)))
        finally:
            cursor.close()

    def has_enough_money(self,acctid,money):
        cursor = self.conn.cursor()
        try:
            sql = "select * from account where acctid=%s and money>%s"%{acctid,money}
            cursor.execute(sql)
            print('has_enough_money:'+sql)
            rs = cursor.fectchall()
            if len(rs) != 1:
                raise (Exception('帳號%s沒有足夠的金額'%(acctid)))
        finally:
            cursor.close()


    def reduce_money(self,acctid,money):
        cursor = self.conn.cursor()
        try:
            sql = "update account set money = money-%s where acctid=%s"%{money,acctid}
            cursor.execute(sql)
            print('reduce_money:'+sql)
            if cursor.rowcount != 1:
                raise (Exception('帳號%s減款失敗'%(acctid)))
        finally:
            cursor.close()

    def add_money(self,acctid,money):
        cursor = self.conn.cursor()
        try:
            sql = "update account set money = money+%s where acctid=%s"%{money,acctid}
            cursor.execute(sql)
            print('reduce_money:'+sql)
            if cursor.rowcount != 1:
                raise (Exception('帳號%s加款失敗'%(acctid)))
        finally:
            cursor.close()


    def transfer(self,source_acctid,target_acctid,money):

        try:
            self.check_acct_available(source_acctid)
            self.check_acct_available(target_acctid)
            self.has_enough_money(source_acctid,money)
            self.reduce_money(source_acctid,money)
            self.add_money(target_acctid,money)
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            print ('Error',e)



if __name__ =='__main__':
    source_acctid = sys.argv[1]
    target_acctid = sys.argv[2]
    money = sys.argv[3]

    conn = pymysql.connect ('localhost','root','','myflaskpp')

    trans_money = TransferMonery(conn)

    try:
        trans_money.transfer(source_acctid,target_acctid,money)

    except Exception as e:
        print ('Error %s: %s'%{e[0],e[1]})

    finally:
        conn.close()

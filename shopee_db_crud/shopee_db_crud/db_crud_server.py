import mysql.connector
import rclpy
from shopee_db_interface.srv import Query #서비스 인터페이스, 개인 설정에 따라 변경
from rclpy.node import Node


# DB 연결 개인 환경에 따라 변경
conn = mysql.connector.connect(
    host="192.168.0.138",
    user="user1",
    password="1234",
    database="shopee_db"
)

cursor = conn.cursor()

class DBCRUDService(Node):

    def __init__(self):
        super().__init__('minimal_service')
        self.srv = self.create_service(Query, 'DBCRUD', self.DB_CRUD_callback)
        print('Server started...')

    def DB_CRUD_callback(self, request, response):

        if request.action.lower() == "insert": # 데이터 생성, table, action, data = "location", "insert", "(2, 2.0, 3.0, 4)"
            
            print('insert start')
            self.insert(request, response)

        elif request.action.lower() == "select": # 데이터 읽기, table, action, data = "location", "select", "id = 2"
            
            print('select start')
            self.select(request, response)
        
        elif request.action.lower() == "update": # 데이터 수정, table, action, data = "location", "update", "location_x = 9.0, location_y = 9.0, aruco_marker = 9 / id = 3"
            
            print('update start')
            self.update(request, response)

        elif request.action.lower() == "delete": # 데이터 삭제, table, action, data = "location", "delete", "id > 0"
            
            print('delete start')
            self.delete(request, response)
            
        else:
            response.result = 'wrong action request'
        
        print(response.result)
        return response
    
    def insert(self, request, response):

        sql = "INSERT INTO " + request.table + " VALUES " + request.data + ";" # string 형태로 쿼리문 만들기
        print('table : ' + request.table + ", action : " + request.action + ", data : " + request.data)
            
        try:

            cursor.execute(sql) # SQL 실행
            conn.commit()  # 변경 사항 저장
            response.result = 'insert success'

        except mysql.connector.Error as err:

            response.result = self.error_report(sql, err)

    def select(self, request, response):

        sql = "SELECT * FROM " + request.table + " WHERE " + request.data + ";" # string 형태로 쿼리문 만들기
        print('table : ' + request.table + ", action : " + request.action + ", data : " + request.data)
        
        try:
                
            cursor.execute(sql) # SQL 실행
            rows = cursor.fetchall()  # 모든 행 가져오기
                
            for row in rows:
                print(row)  # 튜플 형태로 출력
                
            response.result = str(rows) # 튜플 형태를 string 형태로 바꿔서 response에 저장
            print(response.result)
            print(type(response.result))

        except mysql.connector.Error as err:

            response.result = self.error_report(sql, err)

    def update(self, request, response):
        
        set = request.data.split(" / ")[0] # request.data는 "수정할 컬럼과 값 / 변경할 row 조건" 형태로 받음
        where = request.data.split(" / ")[1]
        sql = "UPDATE " + request.table + " SET " + set + " WHERE " + where + ";" # string 형태로 쿼리문 만들기
        print('table : ' + request.table + ", action : " + request.action + ", data : " + request.data)

        try:

            cursor.execute(sql) # SQL 실행
            conn.commit()  # 변경 사항 저장
            response.result = 'update success'

        except mysql.connector.Error as err:

            response.result = self.error_report(sql, err)

    def delete(self, request, response):

        sql = "DELETE FROM " + request.table + " WHERE " + request.data + ";" # string 형태로 쿼리문 만들기
        print('table : ' + request.table + ", action : " + request.action + ", data : " + request.data)
            
        try:
                
            cursor.execute(sql) # SQL 실행
            conn.commit()  # 변경 사항 저장
            response.result = 'delete success'

        except mysql.connector.Error as err:

            response.result = self.error_report(sql, err)

    def error_report(self, sql, err):

        print("sql : ", sql)
        print("Error Code:", err.errno)
        print("Error Message:", err.msg)
        result = 'fail'
        conn.rollback()  # 오류 발생 시 롤백
        
        return result
        

def main():
    rclpy.init()

    minimal_service = DBCRUDService()

    rclpy.spin(minimal_service)

    rclpy.shutdown()


if __name__ == '__main__': # db_s
    main()
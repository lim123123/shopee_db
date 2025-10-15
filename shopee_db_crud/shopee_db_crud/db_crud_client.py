import json
import rclpy
import sys
from shopee_db_interface.srv import Query #서비스 인터페이스, 개인 설정에 따라 변경
from rclpy.node import Node



class DBClient(Node):

    def __init__(self):
        super().__init__('minimal_client_async')
        self.cli = self.create_client(Query, 'DBCRUD')
        while not self.cli.wait_for_service(timeout_sec=1.0):
            self.get_logger().info('service not available, waiting again...')
        self.req = Query.Request()

    def send_request(self, table, action, data):
        self.req.table = table
        self.req.action = action
        self.req.data = data
        return self.cli.call_async(self.req)


def main():
    rclpy.init()

    db_client = DBClient()

    table, action, data = "location", "select", "location_id > 0" # 테이블, 작업, 읽기조건, 모든 컬럼 주는걸로, 데이터 읽기 조건
    # table, action, data = "location", "delete", "id = 5" # 테이블, 작업, 삭제조건, id > 0은 전체 삭제, 아니면 삭제조건 설정
    # table, action, data = "location", "insert", "(1, 99.0, 8.0, 4)" # 테이블, 작업, 생성조건, (id, location_x, location_y, aruco_marker)
    # table, action, data = "location", "update", "location_x = 5.0, location_y = 9.0, aruco_marker = 1 / id = 2" # 테이블, 작업, "수정할 컬럼과 값 / 변경할 row 조건"
    future = db_client.send_request(table, action, data)

    rclpy.spin_until_future_complete(db_client, future)
    response = future.result()

    try: # select 결과가 json 형태로 오기 때문에 파싱

        data = json.loads(response.result)

    except json.JSONDecodeError: # select 이외의 결과는 그냥 문자열
        
        data = response.result

    print('Response: %s' % data)

    db_client.destroy_node()
    rclpy.shutdown()


if __name__ == '__main__':
    main()
"""
DaMeng dialect测试文件
"""
from tests.dialects.test_dialect import Validator


class TestDameng(Validator):
    """测试DaMeng数据库方言"""
    
    dialect = "dameng"
    
    def test_basic_select(self):
        """测试基本的SELECT语句"""
        self.validate_identity("SELECT * FROM table1")
        self.validate_identity("SELECT id, name FROM users")
        
    def test_data_types(self):
        """测试数据类型映射"""
        self.validate_identity("CREATE TABLE test (id INT, name VARCHAR(50))")
        self.validate_identity("CREATE TABLE test (id TINYINT, name TEXT)")
        
    def test_mysql_to_dameng_conversion(self):
        """测试MySQL到DaMeng的转换"""
        # 测试基本的SELECT
        self.validate_all(
            "SELECT * FROM users",
            write={
                "dameng": "SELECT * FROM users",
                "mysql": "SELECT * FROM users"
            }
        )
        
        # 测试数据类型转换
        self.validate_all(
            "CREATE TABLE test (id MEDIUMINT, name TINYTEXT)",
            write={
                "dameng": "CREATE TABLE test (id INT, name TEXT)",
                "mysql": "CREATE TABLE test (id MEDIUMINT, name TINYTEXT)"
            }
        )
        
    def test_functions(self):
        """测试函数转换"""
        self.validate_identity("SELECT CONCAT('Hello', ' World')")
        self.validate_identity("SELECT LENGTH('test')")
        self.validate_identity("SELECT UPPER('hello')")
        self.validate_identity("SELECT LOWER('HELLO')")
        
    def test_limit_offset(self):
        """测试LIMIT和OFFSET"""
        self.validate_identity("SELECT * FROM users LIMIT 10")
        self.validate_identity("SELECT * FROM users LIMIT 10 OFFSET 5")
        
    def test_current_timestamp(self):
        """测试当前时间函数"""
        self.validate_identity("SELECT CURRENT_TIMESTAMP")
        self.validate_identity("SELECT CURRENT_DATE")
        self.validate_identity("SELECT CURRENT_TIME") 
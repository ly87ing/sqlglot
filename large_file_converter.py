
import sqlglot
import re
import logging
from sqlglot import transpile
from sqlglot.dialects.dameng import Dameng
from sqlglot.expressions import Expression

def setup_logging():
    """配置日志记录，同时输出到文件和控制台。"""
    # 清理旧的处理器，避免重复记录
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename='conversion.log',
        filemode='w'
    )
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)

def preprocess_sql_statement(sql: str) -> str:
    """
    对单条SQL语句应用所有预处理规则。
    """
    processed_sql = sql
    original_sql = sql

    # 规则1: 移除 USING BTREE
    processed_sql = re.sub(r"USING\s+BTREE", "", processed_sql, flags=re.IGNORECASE)

    # 规则2: 移除 ON UPDATE CURRENT_TIMESTAMP
    processed_sql = re.sub(r"ON\s+UPDATE\s+CURRENT_TIMESTAMP", "", processed_sql, flags=re.IGNORECASE)

    # 规则3: 移除 ROW_FORMAT=...
    processed_sql = re.sub(r"ROW_FORMAT\s*=\s*\w+", "", processed_sql, flags=re.IGNORECASE)

    # 规则4: 移除 AUTO_INCREMENT=... 表选项
    # 使用正则表达式确保我们只移除表选项，而不是列属性
    processed_sql = re.sub(r"AUTO_INCREMENT\s*=\s*\d+", "", processed_sql, flags=re.IGNORECASE)

    if processed_sql != original_sql:
        logging.info(f"预处理规则已应用。原始: '{original_sql[:100]}...' -> 预处理后: '{processed_sql[:100]}...'")

    return processed_sql

def convert_mysql_to_dameng(input_file, output_file):
    """
    读取MySQL SQL文件，使用“失败后回退重试”策略进行转换。
    """
    setup_logging()
    logging.info(f"开始转换 {input_file} (v6 - 失败回退策略)...")
    
    successful_direct = 0
    successful_preprocessed = 0
    failed_conversions = 0
    
    dameng_dialect = Dameng()
    
    try:
        with open(input_file, 'r', encoding='utf-8') as f_in, open(output_file, 'w', encoding='utf-8') as f_out:
            sql_content = f_in.read()
            
            # 全局预处理步骤: 在解析前替换字符集
            logging.info("全局预处理: 正在将 'utf8mb4' 替换为 'UTF8'...")
            original_size = len(sql_content)
            sql_content = re.sub(r'utf8mb4', 'UTF8', sql_content, flags=re.IGNORECASE)
            if len(sql_content) != original_size:
                logging.info("字符集替换完成。")

            expressions = sqlglot.parse(sql_content, read='mysql', error_level='ignore')
            total_statements = len(expressions)
            logging.info(f"共检测到 {total_statements} 条SQL语句。")
            
            for i, expr in enumerate(expressions):
                if not isinstance(expr, Expression):
                    failed_conversions += 1
                    logging.warning(f"语句 {i+1}/{total_statements} 无法解析，已跳过。内容: {str(expr)[:100]}...")
                    continue

                original_sql = expr.sql()
                
                try:
                    # 首次尝试直接转换
                    dameng_sql = transpile(original_sql, read='mysql', write=dameng_dialect, pretty=True)[0]
                    f_out.write(dameng_sql + ';\n')
                    successful_direct += 1
                except Exception as e:
                    # 如果直接转换失败，则尝试预处理后重试
                    logging.warning(f"语句 {i+1}/{total_statements} 直接转换失败: {e}。尝试预处理...")
                    
                    try:
                        preprocessed_sql = preprocess_sql_statement(original_sql)
                        dameng_sql = transpile(preprocessed_sql, read='mysql', write=dameng_dialect, pretty=True)[0]
                        f_out.write(dameng_sql + ';\n')
                        successful_preprocessed += 1
                    except Exception as e2:
                        failed_conversions += 1
                        logging.error(
                            f"语句 {i+1}/{total_statements} 预处理后依然转换失败。错误: {e2}\n"
                            f"--- 原始SQL ---\n{original_sql}\n"
                            "-------------------"
                        )

        logging.info("--- 转换统计 ---")
        logging.info(f"总语句数: {total_statements}")
        logging.info(f"直接成功: {successful_direct}")
        logging.info(f"预处理后成功: {successful_preprocessed}")
        logging.info(f"失败: {failed_conversions}")
        success_rate = (successful_direct + successful_preprocessed) / total_statements * 100
        logging.info(f"总成功率: {success_rate:.2f}%")
        logging.info(f"转换完成！结果已保存至 {output_file}。详情请查看 conversion.log。")
        
    except FileNotFoundError:
        logging.error(f"错误: 输入文件 {input_file} 未找到。")
    except Exception as e:
        logging.error(f"处理过程中发生未知错误: {e}", exc_info=True)

if __name__ == "__main__":
    convert_mysql_to_dameng('mysql_ainemo.sql', 'mysql_postgres.sql') 
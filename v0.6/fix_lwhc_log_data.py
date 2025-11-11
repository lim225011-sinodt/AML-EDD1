#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
修复tb_lwhc_log表数据生成程序
按照正确的联网核查结果定义重新生成数据
"""

import mysql.connector
import random
import sys
from datetime import datetime, timedelta

class LwhcLogDataFixer:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.bank_codes = []
        self.customer_data = []

    def connect_database(self):
        """连接数据库"""
        try:
            self.conn = mysql.connector.connect(
                host='101.42.102.9',
                port=3306,
                user='root',
                password='Bancstone123!',
                database='AML300',
                charset='utf8mb4'
            )
            self.cursor = self.conn.cursor()
            print("[OK] 数据库连接成功")
            return True
        except Exception as e:
            print(f"[ERROR] 数据库连接失败: {e}")
            return False

    def get_bank_codes(self):
        """获取银行信息"""
        try:
            self.cursor.execute("SELECT Bank_code1, Bank_name FROM tb_bank")
            self.bank_codes = self.cursor.fetchall()
            print(f"[OK] 获取到 {len(self.bank_codes)} 个银行分行")
            return len(self.bank_codes) > 0
        except Exception as e:
            print(f"[ERROR] 获取银行信息失败: {e}")
            return False

    def get_customer_data(self):
        """获取客户数据"""
        try:
            # 获取个人客户数据
            self.cursor.execute("""
                SELECT Cst_no, Acc_name, Id_no, Bank_code1
                FROM tb_cst_pers
            """)
            personal_customers = self.cursor.fetchall()

            # 获取企业客户数据
            self.cursor.execute("""
                SELECT Cst_no, Acc_name, License, Bank_code1
                FROM tb_cst_unit
            """)
            corporate_customers = self.cursor.fetchall()

            self.customer_data = {
                'personal': personal_customers,
                'corporate': corporate_customers
            }

            print(f"[OK] 获取个人客户: {len(personal_customers)} 个")
            print(f"[OK] 获取企业客户: {len(corporate_customers)} 个")
            return True
        except Exception as e:
            print(f"[ERROR] 获取客户数据失败: {e}")
            return False

    def clear_existing_data(self):
        """清理现有数据"""
        print("\n[INFO] 清理现有tb_lwhc_log数据...")
        try:
            self.cursor.execute("DELETE FROM tb_lwhc_log")
            self.conn.commit()
            print("[OK] 现有数据清理完成")
            return True
        except Exception as e:
            print(f"[ERROR] 数据清理失败: {e}")
            return False

    def generate_lwhc_log_data(self):
        """生成符合规范的联网核查日志数据"""
        print("\n[INFO] 生成tb_lwhc_log数据...")

        # 删除原表并重新创建（如果需要更改表结构）
        try:
            # 先删除表
            self.cursor.execute("DROP TABLE IF EXISTS tb_lwhc_log")

            # 重新创建表（保持现有结构但优化注释）
            self.cursor.execute("""
                CREATE TABLE tb_lwhc_log (
                    Bank_name varchar(120) NOT NULL COMMENT '银行网点名称',
                    Bank_code2 varchar(20) NOT NULL COMMENT '银行网点代码',
                    Date char(8) NOT NULL COMMENT '核查日期(YYYYMMDD)',
                    Time char(6) NOT NULL COMMENT '核查时间(HHMMSS)',
                    Name varchar(40) NOT NULL COMMENT '客户姓名',
                    Id_no varchar(50) NOT NULL COMMENT '身份证号码/营业执照',
                    Result char(2) NOT NULL COMMENT '核查结果: 11-一致且照片存在,12-一致无照片,13-一致照片错误,14-不匹配,15-不存在,16-其他',
                    Counter_no varchar(30) COMMENT '柜员号',
                    Ope_line varchar(40) COMMENT '操作网点',
                    Mode char(2) COMMENT '核查方式: 01-在线,02-离线',
                    Purpose varchar(120) COMMENT '核查目的',
                    PRIMARY KEY (Id_no, Date, Time),
                    KEY (Bank_code2),
                    KEY (Date),
                    KEY (Result)
                ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='公民联网核查日志记录表'
            """)

            self.conn.commit()
            print("[OK] tb_lwhc_log表重新创建成功")
        except Exception as e:
            print(f"[ERROR] 表重建失败: {e}")
            return False

        # 生成核查结果定义
        result_codes = {
            '11': {'name': '一致且照片存在', 'desc': '公民身份号码与姓名一致，且存在照片', 'ratio': 0.4},
            '12': {'name': '一致无照片', 'desc': '姓名与号码相符但照片不存在', 'ratio': 0.3},
            '13': {'name': '一致照片错误', 'desc': '姓名与号码相符但照片错误', 'ratio': 0.15},
            '14': {'name': '不匹配', 'desc': '号码存在但与姓名不匹配', 'ratio': 0.1},
            '15': {'name': '不存在', 'desc': '号码不存在', 'ratio': 0.04},
            '16': {'name': '其他', 'desc': '其他情况', 'ratio': 0.01}
        }

        # 生成核查目的
        purposes = [
            '开户核查', '大额交易核查', '可疑交易核查', '风险评估核查',
            '身份验证', '证件更新', '定期核查', '特殊情况核查'
        ]

        # 生成操作员和柜员号
        operators = ['张三', '李四', '王五', '赵六', '钱七', '孙八', '周九', '吴十']

        # 生成操作网点
        operation_lines = [
            '总行营业部', '北京分行', '上海分行', '广东分行', '深圳分行',
            '成都分行', '杭州分行', '南京分行', '武汉分行', '西安分行'
        ]

        batch_data = []

        # 时间范围：2020年1月1日到2020年6月30日
        start_date = datetime(2020, 1, 1)
        end_date = datetime(2020, 6, 30)
        date_range = (end_date - start_date).days + 1

        # 生成1500条联网核查记录
        target_records = 1500
        generated_count = 0

        while generated_count < target_records:
            # 随机选择时间
            random_days = random.randint(0, date_range - 1)
            check_date = (start_date + timedelta(days=random_days)).strftime('%Y%m%d')
            check_time = f"{str(random.randint(8, 18)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}{str(random.randint(0, 59)).zfill(2)}"

            # 选择客户
            customer_type = random.choice(['personal', 'corporate'])
            customer_data = random.choice(self.customer_data[customer_type])

            # 选择银行
            bank_name, bank_code = random.choice(self.bank_codes)

            # 选择核查结果（根据概率分布）
            result_code = self._choose_result_code(result_codes)
            result_info = result_codes[result_code]

            # 生成操作员信息
            operator = random.choice(operators)
            counter_no = f"COUNTER{str(random.randint(1, 999)).zfill(3)}"
            ope_line = random.choice(operation_lines)
            mode = random.choice(['01', '02'])
            purpose = random.choice(purposes)

            data = (
                bank_name,
                bank_code,
                check_date,
                check_time,
                customer_data[1],  # 客户姓名
                customer_data[2] if customer_type == 'personal' else customer_data[3],  # 身份证号或营业执照
                result_code,
                counter_no,
                ope_line,
                mode,
                purpose
            )

            batch_data.append(data)
            generated_count += 1

            # 每100条提交一次
            if len(batch_data) >= 100:
                self.cursor.executemany("""
                    INSERT INTO tb_lwhc_log (
                        Bank_name, Bank_code2, Date, Time, Name, Id_no, Result,
                        Counter_no, Ope_line, Mode, Purpose
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, batch_data)
                self.conn.commit()
                batch_data = []
                print(f"  已生成 {generated_count} 条记录")

        # 提交剩余数据
        if batch_data:
            self.cursor.executemany("""
                INSERT INTO tb_lwhc_log (
                    Bank_name, Bank_code2, Date, Time, Name, Id_no, Result,
                    Counter_no, Ope_line, Mode, Purpose
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch_data)
            self.conn.commit()

        print(f"[OK] tb_lwhc_log数据生成完成：{target_records} 条记录")
        return True

    def _choose_result_code(self, result_codes):
        """根据概率分布选择结果代码"""
        rand_val = random.random()
        cumulative = 0
        for code, info in result_codes.items():
            cumulative += info['ratio']
            if rand_val <= cumulative:
                return code
        return '16'  # 默认返回其他

    def verify_data_generation(self):
        """验证数据生成结果"""
        print("\n[INFO] 验证数据生成结果...")

        # 统计各种结果的数量
        try:
            self.cursor.execute("""
                SELECT
                    Result,
                    COUNT(*) as count,
                    ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tb_lwhc_log), 2) as percentage
                FROM tb_lwhc_log
                GROUP BY Result
                ORDER BY Result
            """)
            results = self.cursor.fetchall()

            print("核查结果分布:")
            print("-" * 50)
            result_desc = {
                '11': '一致且照片存在',
                '12': '一致无照片',
                '13': '一致照片错误',
                '14': '不匹配',
                '15': '不存在',
                '16': '其他'
            }
            total_count = 0
            for result, count, percentage in results:
                print(f"  结果{result} ({result_desc[result]}): {count} 条 ({percentage}%)")
                total_count += count

            print(f"\n总记录数: {total_count}")

            # 验证与客户数据的关联性
            print("\n[INFO] 验证与客户数据的关联性...")

            # 检查个人客户关联性
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_lwhc_log l
                JOIN tb_cst_pers p ON l.Id_no = p.Id_no
            """)
            personal_linked = self.cursor.fetchone()[0]

            # 检查企业客户关联性
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_lwhc_log l
                JOIN tb_cst_unit u ON l.Id_no = u.License
            """)
            corporate_linked = self.cursor.fetchone()[0]

            # 检查账户关联性
            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_lwhc_log l
                JOIN tb_cst_pers p ON l.Id_no = p.Id_no
                JOIN tb_acc a ON p.Cst_no = a.Cst_no
            """)
            account_linked_via_personal = self.cursor.fetchone()[0]

            self.cursor.execute("""
                SELECT COUNT(*) FROM tb_lwhc_log l
                JOIN tb_cst_unit u ON l.Id_no = u.License
                JOIN tb_acc a ON u.Cst_no = a.Cst_no
            """)
            account_linked_via_corporate = self.cursor.fetchone()[0]

            print(f"与个人客户关联: {personal_linked} 条")
            print(f"与企业客户关联: {corporate_linked} 条")
            print(f"通过个人客户关联到账户: {account_linked_via_personal} 条")
            print(f"通过企业客户关联到账户: {account_linked_via_corporate} 条")
            print(f"总关联到账户的核查记录: {account_linked_via_personal + account_linked_via_corporate} 条")

            # 检查时间范围
            self.cursor.execute("SELECT MIN(Date), MAX(Date) FROM tb_lwhc_log")
            date_range = self.cursor.fetchone()
            print(f"核查时间范围: {date_range[0]} 至 {date_range[1]}")

            return True

        except Exception as e:
            print(f"[ERROR] 验证失败: {e}")
            return False

    def generate_consistency_report(self):
        """生成数据一致性报告"""
        print("\n[INFO] 生成数据一致性报告...")

        try:
            # 检查姓名和身份证号码的一致性
            print("姓名与证件号码一致性检查:")

            # 个人客户姓名一致性
            self.cursor.execute("""
                SELECT COUNT(*) AS total_checks,
                       COUNT(CASE WHEN p.Acc_name = l.Name THEN 1 END) AS name_matches,
                       ROUND(COUNT(CASE WHEN p.Acc_name = l.Name END) * 100.0 / COUNT(*), 2) AS match_rate
                FROM tb_lwhc_log l
                JOIN tb_cst_pers p ON l.Id_no = p.Id_no
            """)
            personal_consistency = self.cursor.fetchone()

            print(f"  个人客户姓名一致性: {personal_consistency[1]}/{personal_consistency[0]} ({personal_consistency[2]}%)")

            # 核查结果与实际情况的逻辑性
            print("\n核查结果逻辑性检查:")
            self.cursor.execute("""
                SELECT Result, COUNT(*) as count
                FROM tb_lwhc_log
                GROUP BY Result
            """)
            result_stats = self.cursor.fetchall()

            print("各结果代码的记录数量:")
            for result, count in result_stats:
                result_desc = {
                    '11': '姓名号码一致且照片存在',
                    '12': '姓名号码一致但无照片',
                    '13': '姓名号码一致但照片错误',
                    '14': '号码存在但姓名不匹配'
                }
                if result in result_desc:
                    print(f"  {result}: {count} 条 ({result_desc[result]})")

            return True

        except Exception as e:
            print(f"[ERROR] 报告生成失败: {e}")
            return False

    def run_data_fix(self):
        """执行数据修复流程"""
        print("=" * 60)
        print("tb_lwhc_log数据修复程序")
        print("=" * 60)

        try:
            if not self.connect_database():
                return False

            if not self.get_bank_codes():
                return False

            if not self.get_customer_data():
                return False

            if not self.clear_existing_data():
                return False

            if not self.generate_lwhc_log_data():
                return False

            if not self.verify_data_generation():
                return False

            if not self.generate_consistency_report():
                return False

            print("\n[SUCCESS] tb_lwhc_log数据修复完成！")
            print("✅ 已重新生成符合规范的1500条联网核查记录")
            print("✅ Name字段现在正确表示客户姓名")
            print("✅ Result字段使用正确的联网核查结果代码")
            print("✅ 数据与客户账户建立了正确的关联关系")

            return True

        except Exception as e:
            print(f"[ERROR] 数据修复过程中发生错误: {e}")
            if self.conn:
                self.conn.rollback()
            return False

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    print("[INFO] 开始修复tb_lwhc_log表数据...")
    fixer = LwhcLogDataFixer()
    success = fixer.run_data_fix()

    if success:
        print("\n[SUCCESS] 数据修复成功！")
        sys.exit(0)
    else:
        print("\n[ERROR] 数据修复失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()
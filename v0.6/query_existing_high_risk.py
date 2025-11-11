#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询当前数据库中的高风险客户
查询所有被认定为高风险（代码：01）的客户
"""

import mysql.connector
import sys
from datetime import datetime

def query_existing_high_risk_customers():
    """查询现有的高风险客户"""
    print("="*80)
    print("高风险客户查询报告")
    print("查询所有风险等级为01（高风险）的客户")
    print("="*80)

    try:
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='AML300',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        # 首先查询风险评估时间范围
        print("\n[风险评估时间范围]")
        print("-" * 40)

        cursor.execute("""
            SELECT MIN(Time) AS 最早评估时间, MAX(Time) AS 最晚评估时间, COUNT(*) AS 总记录数
            FROM tb_risk_new
            WHERE Risk_code = '01'
        """)

        time_range = cursor.fetchone()
        if time_range[0]:
            print(f"最早评估时间: {time_range[0]}")
            print(f"最晚评估时间: {time_range[1]}")
            print(f"高风险客户总数: {time_range[2]} 个")

        # 查询个人高风险客户
        print("\n[个人高风险客户]")
        print("-" * 80)

        personal_sql = """
        SELECT
            p.Cst_no AS 客户号,
            p.Acc_name AS 客户名称,
            p.Id_no AS 证件号码,
            r.Risk_code AS 风险等级代码,
            r.Time AS 评估时间,
            p.Bank_code1 AS 所属分行代码,
            (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = p.Bank_code1) AS 所属分行名称,
            p.Contact1 AS 联系方式,
            p.Occupation AS 职业,
            p.Income AS 年收入,
            r.Norm AS 评估说明
        FROM tb_risk_new r
        JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
        WHERE r.Risk_code = '01'
          AND r.Acc_type = '11'
        ORDER BY p.Cst_no
        """

        cursor.execute(personal_sql)
        personal_results = cursor.fetchall()

        if personal_results:
            print(f"{'客户号':<12} {'客户名称':<15} {'证件号码':<20} {'评估时间':<12} {'所属分行':<15} {'职业':<10}")
            print("-" * 95)
            for i, row in enumerate(personal_results, 1):
                print(f"{row[0]:<12} {row[1]:<15} {row[2]:<20} {row[4]:<12} {row[6]:<15} {row[8]:<10}")
                if i <= 5:  # 显示前5条评估说明
                    print(f"           评估说明: {row[10]}")
            if len(personal_results) > 5:
                print(f"           ... 还有{len(personal_results)-5}个客户的详细信息")
            print(f"\n小计：{len(personal_results)} 个个人高风险客户")
        else:
            print("未找到个人高风险客户")

        # 查询企业高风险客户
        print("\n[企业高风险客户]")
        print("-" * 80)

        corporate_sql = """
        SELECT
            u.Cst_no AS 客户号,
            u.Acc_name AS 客户名称,
            u.License AS 统一社会信用代码,
            r.Risk_code AS 风险等级代码,
            r.Time AS 评估时间,
            u.Bank_code1 AS 所属分行代码,
            (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = u.Bank_code1) AS 所属分行名称,
            u.Rep_name AS 法定代表人,
            u.Industry AS 行业,
            u.Reg_amt AS 注册资本,
            r.Norm AS 评估说明
        FROM tb_risk_new r
        JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
        WHERE r.Risk_code = '01'
          AND r.Acc_type = '13'
        ORDER BY u.Cst_no
        """

        cursor.execute(corporate_sql)
        corporate_results = cursor.fetchall()

        if corporate_results:
            print(f"{'客户号':<12} {'客户名称':<20} {'统一社会信用代码':<20} {'评估时间':<12} {'所属分行':<15} {'行业':<10}")
            print("-" * 100)
            for i, row in enumerate(corporate_results, 1):
                print(f"{row[0]:<12} {row[1]:<20} {row[2]:<20} {row[4]:<12} {row[6]:<15} {row[8]:<10}")
                if i <= 5:  # 显示前5条评估说明
                    print(f"           评估说明: {row[10]}")
            if len(corporate_results) > 5:
                print(f"           ... 还有{len(corporate_results)-5}个企业的详细信息")
            print(f"\n小计：{len(corporate_results)} 个企业高风险客户")
        else:
            print("未找到企业高风险客户")

        # 统计信息
        print("\n[统计信息]")
        print("-" * 80)

        total_high_risk = len(personal_results) + len(corporate_results)
        print(f"个人高风险客户数量：{len(personal_results)} 个")
        print(f"企业高风险客户数量：{len(corporate_results)} 个")
        print(f"高风险客户总数：{total_high_risk} 个")

        # 查询总客户数以计算比例
        cursor.execute("SELECT COUNT(*) FROM tb_risk_new")
        total_customers = cursor.fetchone()[0]

        if total_customers > 0:
            high_risk_percentage = (total_high_risk / total_customers) * 100
            print(f"总客户数：{total_customers} 个")
            print(f"高风险客户占比：{high_risk_percentage:.2f}%")

        # 按分行统计高风险客户
        print("\n[按分行统计高风险客户]")
        print("-" * 50)

        branch_stats_sql = """
        SELECT
            b.Bank_name AS 分行名称,
            COUNT(*) AS 高风险客户数量
        FROM tb_risk_new r
        JOIN tb_bank b ON r.Bank_code1 = b.Bank_code1
        WHERE r.Risk_code = '01'
        GROUP BY b.Bank_name, b.Bank_code1
        ORDER BY COUNT(*) DESC
        """

        cursor.execute(branch_stats_sql)
        branch_results = cursor.fetchall()

        if branch_results:
            print(f"{'分行名称':<20} {'高风险客户数量':<15}")
            print("-" * 40)
            for row in branch_results:
                print(f"{row[0]:<20} {row[1]:<15}")

        cursor.close()
        conn.close()

        return total_high_risk > 0

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def query_risk_level_distribution():
    """查询风险等级分布情况"""
    print("\n" + "="*80)
    print("风险等级分布统计")
    print("="*80)

    try:
        conn = mysql.connector.connect(
            host='101.42.102.9',
            port=3306,
            user='root',
            password='Bancstone123!',
            database='AML300',
            charset='utf8mb4'
        )

        cursor = conn.cursor()

        cursor.execute("""
            SELECT
                Risk_code AS 风险等级代码,
                CASE Risk_code
                    WHEN '01' THEN '高风险'
                    WHEN '02' THEN '中高风险'
                    WHEN '03' THEN '中风险'
                    WHEN '04' THEN '低风险'
                    ELSE '未知'
                END AS 风险等级描述,
                COUNT(*) AS 客户数量,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM tb_risk_new), 2) AS 占比
            FROM tb_risk_new
            GROUP BY Risk_code
            ORDER BY Risk_code
        """)

        risk_distribution = cursor.fetchall()

        print(f"{'风险等级代码':<8} {'风险等级描述':<10} {'客户数量':<10} {'占比(%)':<10}")
        print("-" * 45)
        for row in risk_distribution:
            print(f"{row[0]:<8} {row[1]:<10} {row[2]:<10} {row[3]:<10}%")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"[ERROR] 风险分布查询失败: {e}")
        return False

def main():
    """主函数"""
    print("[INFO] 开始查询高风险客户数据...")
    print(f"[INFO] 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 查询基本信息
    success1 = query_existing_high_risk_customers()

    # 查询风险等级分布
    success2 = query_risk_level_distribution()

    if success1 and success2:
        print("\n[SUCCESS] 高风险客户查询完成！")
        print("请查看以上详细结果。")
        sys.exit(0)
    else:
        print("\n[ERROR] 查询过程中发生错误，请检查数据库连接。")
        sys.exit(1)

if __name__ == "__main__":
    main()
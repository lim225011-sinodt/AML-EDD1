#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
查询高风险客户程序
查询2024年1月1日到2024年6月1日期间被认定为高风险的客户
"""

import mysql.connector
import sys
from datetime import datetime

def query_high_risk_customers():
    """查询高风险客户"""
    print("="*80)
    print("高风险客户查询报告")
    print("查询期间：2024年1月1日 - 2024年6月1日")
    print("风险等级：高风险（代码：01）")
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

        # 查询个人高风险客户
        print("\n[个人高风险客户]")
        print("-" * 80)

        personal_sql = """
        SELECT
            '个人客户' AS 客户类型,
            p.Cst_no AS 客户号,
            p.Acc_name AS 客户名称,
            p.Id_no AS 证件号码,
            r.Risk_code AS 风险等级代码,
            CASE r.Risk_code
                WHEN '01' THEN '高风险'
                WHEN '02' THEN '中高风险'
                WHEN '03' THEN '中风险'
                WHEN '04' THEN '低风险'
                ELSE '未知'
            END AS 风险等级描述,
            r.Time AS 评估时间,
            p.Bank_code1 AS 所属分行代码,
            (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = p.Bank_code1) AS 所属分行名称,
            p.Contact1 AS 联系方式,
            p.Occupation AS 职业,
            p.Income AS 年收入
        FROM tb_risk_new r
        JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
        WHERE r.Risk_code = '01'
          AND r.Time BETWEEN '20240101' AND '20240601'
          AND r.Acc_type = '11'
        ORDER BY p.Cst_no
        """

        cursor.execute(personal_sql)
        personal_results = cursor.fetchall()

        if personal_results:
            print(f"{'客户号':<12} {'客户名称':<20} {'证件号码':<20} {'风险评估时间':<12} {'所属分行':<15} {'职业':<10}")
            print("-" * 100)
            for row in personal_results:
                print(f"{row[1]:<12} {row[2]:<20} {row[3]:<20} {row[6]:<12} {row[8]:<15} {row[10]:<10}")
            print(f"\n小计：{len(personal_results)} 个个人高风险客户")
        else:
            print("未找到个人高风险客户")

        # 查询企业高风险客户
        print("\n[企业高风险客户]")
        print("-" * 80)

        corporate_sql = """
        SELECT
            '企业客户' AS 客户类型,
            u.Cst_no AS 客户号,
            u.Acc_name AS 客户名称,
            u.License AS 证件号码,
            r.Risk_code AS 风险等级代码,
            CASE r.Risk_code
                WHEN '01' THEN '高风险'
                WHEN '02' THEN '中高风险'
                WHEN '03' THEN '中风险'
                WHEN '04' THEN '低风险'
                ELSE '未知'
            END AS 风险等级描述,
            r.Time AS 评估时间,
            u.Bank_code1 AS 所属分行代码,
            (SELECT b.Bank_name FROM tb_bank b WHERE b.Bank_code1 = u.Bank_code1) AS 所属分行名称,
            u.Rep_name AS 法定代表人,
            u.Industry AS 行业,
            u.Reg_amt AS 注册资本
        FROM tb_risk_new r
        JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no
        WHERE r.Risk_code = '01'
          AND r.Time BETWEEN '20240101' AND '20240601'
          AND r.Acc_type = '13'
        ORDER BY u.Cst_no
        """

        cursor.execute(corporate_sql)
        corporate_results = cursor.fetchall()

        if corporate_results:
            print(f"{'客户号':<12} {'客户名称':<25} {'统一社会信用代码':<20} {'风险评估时间':<12} {'所属分行':<15} {'行业':<10}")
            print("-" * 110)
            for row in corporate_results:
                print(f"{row[1]:<12} {row[2]:<25} {row[3]:<20} {row[6]:<12} {row[8]:<15} {row[10]:<10}")
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
        cursor.execute("SELECT COUNT(*) FROM tb_risk_new WHERE Risk_code = '01'")
        all_high_risk = cursor.fetchone()[0]

        cursor.execute("SELECT COUNT(*) FROM tb_risk_new")
        total_customers = cursor.fetchone()[0]

        if total_customers > 0:
            high_risk_percentage = (all_high_risk / total_customers) * 100
            print(f"总高风险客户数：{all_high_risk} 个")
            print(f"总客户数：{total_customers} 个")
            print(f"高风险客户占比：{high_risk_percentage:.2f}%")

        # 查询期间内的风险评估变化
        print("\n[风险评估变化]")
        print("-" * 80)

        cursor.execute("""
            SELECT Time, COUNT(*) as count
            FROM tb_risk_new
            WHERE Risk_code = '01'
              AND Time BETWEEN '20240101' AND '20240601'
            GROUP BY Time
            ORDER BY Time
        """)

        risk_changes = cursor.fetchall()
        if risk_changes:
            print("评估日期     高风险客户数量")
            print("-" * 30)
            for time, count in risk_changes:
                print(f"{time}        {count} 个")

        cursor.close()
        conn.close()

        return total_high_risk > 0

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return False

def query_high_risk_transaction_analysis():
    """查询高风险客户的交易行为分析"""
    print("\n" + "="*80)
    print("高风险客户交易行为分析")
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

        # 查询高风险客户的交易统计
        print("\n[交易统计]")
        print("-" * 80)

        txn_sql = """
        SELECT
            p.Cst_no AS 客户号,
            p.Acc_name AS 客户名称,
            COUNT(t.Ticd) AS 交易笔数,
            COALESCE(SUM(t.Org_amt), 0) AS 交易总金额,
            COALESCE(AVG(t.Org_amt), 0) AS 平均交易金额,
            COALESCE(MAX(t.Org_amt), 0) AS 最大交易金额
        FROM tb_risk_new r
        JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
        LEFT JOIN tb_acc_txn t ON p.Cst_no = t.Cst_no
            AND t.Date BETWEEN '20240101' AND '20240601'
        WHERE r.Risk_code = '01'
          AND r.Time BETWEEN '20240101' AND '20240601'
          AND r.Acc_type = '11'
        GROUP BY p.Cst_no, p.Acc_name
        ORDER BY COALESCE(SUM(t.Org_amt), 0) DESC
        """

        cursor.execute(txn_sql)
        txn_results = cursor.fetchall()

        if txn_results:
            print(f"{'客户号':<12} {'客户名称':<15} {'交易笔数':<8} {'总金额(元)':<12} {'平均金额(元)':<12} {'最大金额(元)':<12}")
            print("-" * 85)
            total_amount = 0
            for row in txn_results:
                print(f"{row[0]:<12} {row[1]:<15} {row[2]:<8} {row[3]:<12,.2f} {row[4]:<12,.2f} {row[5]:<12,.2f}")
                total_amount += row[3]
            print(f"\n交易总金额合计：{total_amount:,.2f} 元")
        else:
            print("未找到高风险客户的交易记录")

        # 查询跨境交易
        print("\n[跨境交易分析]")
        print("-" * 80)

        cross_border_sql = """
        SELECT
            p.Cst_no AS 客户号,
            p.Acc_name AS 客户名称,
            COUNT(cb.Ticd) AS 跨境交易笔数,
            COALESCE(SUM(cb.Org_amt), 0) AS 跨境交易总金额
        FROM tb_risk_new r
        JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no
        LEFT JOIN tb_cross_border cb ON p.Cst_no = cb.Cst_no
            AND cb.Date BETWEEN '20240101' AND '20240601'
        WHERE r.Risk_code = '01'
          AND r.Time BETWEEN '20240101' AND '20240601'
          AND r.Acc_type = '11'
        GROUP BY p.Cst_no, p.Acc_name
        HAVING COUNT(cb.Ticd) > 0
        ORDER BY COALESCE(SUM(cb.Org_amt), 0) DESC
        """

        cursor.execute(cross_border_sql)
        border_results = cursor.fetchall()

        if border_results:
            print(f"{'客户号':<12} {'客户名称':<15} {'跨境交易笔数':<12} {'跨境总金额(元)':<15}")
            print("-" * 65)
            border_total = 0
            for row in border_results:
                print(f"{row[0]:<12} {row[1]:<15} {row[2]:<12} {row[3]:<15,.2f}")
                border_total += row[3]
            print(f"\n跨境交易总金额合计：{border_total:,.2f} 元")
        else:
            print("未找到高风险客户的跨境交易记录")

        cursor.close()
        conn.close()

        return True

    except Exception as e:
        print(f"[ERROR] 交易分析查询失败: {e}")
        return False

def main():
    """主函数"""
    print("[INFO] 开始查询高风险客户数据...")
    print(f"[INFO] 查询时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    # 查询基本信息
    success1 = query_high_risk_customers()

    # 查询交易分析
    success2 = query_high_risk_transaction_analysis()

    if success1 and success2:
        print("\n[SUCCESS] 高风险客户查询完成！")
        print("请查看以上详细结果。")
        sys.exit(0)
    else:
        print("\n[ERROR] 查询过程中发生错误，请检查数据库连接。")
        sys.exit(1)

if __name__ == "__main__":
    main()
#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高风险客户大额交易报告统计SQL查询
"""

import mysql.connector
import time

def execute_high_risk_lar_analysis():
    """执行高风险客户大额交易报告统计分析"""
    print("="*80)
    print("高风险客户大额交易报告统计分析")
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

        # SQL查询语句
        sql_query = """
        SELECT
            CASE
                WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                WHEN u.Cst_no IS NOT NULL THEN '企业客户'
            END AS 客户类型,
            r.Cst_no AS 客户号,
            COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
            COUNT(lar.RPMN) AS 大额报告数量,
            COALESCE(SUM(lar.Report_Amount), 0) AS 报告总金额,
            MAX(lar.Report_Amount) AS 最大报告金额,
            AVG(lar.Report_Amount) AS 平均报告金额,
            MIN(lar.Report_Amount) AS 最小报告金额,
            COUNT(DISTINCT lar.Report_Date) AS 报告日期数量,
            r.Time AS 风险评估时间
        FROM tb_risk_new r
        LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
        LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
        LEFT JOIN tb_lar_report lar ON
            (lar.Customer_ID = r.Cst_no OR
             lar.Account_No IN (SELECT a.Self_acc_no FROM tb_acc a WHERE a.Cst_no = r.Cst_no))
        WHERE r.Risk_code = '01'
          AND lar.Report_Date BETWEEN '20200101' AND '20200630'
        GROUP BY r.Cst_no, p.Acc_name, u.Acc_name, r.Time,
                 CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
        HAVING COUNT(lar.RPMN) > 0
        ORDER BY COALESCE(SUM(lar.Report_Amount), 0) DESC;
        """

        print("\n[SQL查询语句]")
        print("-" * 50)
        print(sql_query)

        # 执行SQL并计算执行时间
        print("\n[执行SQL查询...")
        print("-" * 30)

        start_time = time.time()
        cursor.execute(sql_query)
        results = cursor.fetchall()
        end_time = time.time()

        execution_time = (end_time - start_time) * 1000  # 转换为毫秒

        print(f"[OK] 执行时间: {execution_time:.2f} 毫秒")
        print(f"[OK] 返回记录数: {len(results)} 条")

        if len(results) > 0:
            print("\n[查询结果]")
            print("-" * 95)
            print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<12} {'报告数':<6} {'总金额(元)':<15} {'最大金额(元)':<12} {'平均金额(元)':<12} {'评估时间':<8}")
            print("-" * 95)

            total_reports = 0
            total_amount = 0
            max_amount = 0
            max_amount_customer = ""

            for row in results:
                customer_type, cst_no, name, report_count, total_amt, max_amt, avg_amt, min_amt, report_days, risk_time = row
                total_reports += report_count
                total_amount += total_amt

                if max_amt > max_amount:
                    max_amount = max_amt
                    max_amount_customer = f"{customer_type}-{cst_no}-{name}"

                print(f"{customer_type:<8} {cst_no:<10} {name[:12]:<12} {report_count:<6} {total_amt:<15,.2f} {max_amt:<12,.2f} {avg_amt:<12,.2f} {risk_time:<8}")

            print("-" * 95)
            print(f"{'总计':<8} {'':<10} {'':<12} {total_reports:<6} {total_amount:<15,.2f}")

            print(f"\n[统计汇总]")
            print("-" * 50)
            print(f"高风险客户总数: 55个")
            print(f"有大额报告的高风险客户: {len(results)}个")
            print(f"覆盖比例: {(len(results)/55)*100:.1f}%")
            print(f"大额报告总数: {total_reports} 份")
            print(f"大额报告总金额: {total_amount:,.2f} 元")
            print(f"平均每个客户报告金额: {total_amount/len(results):,.2f} 元")
            print(f"最高单客户报告金额: {max_amount:,.2f} 元 ({max_amount_customer})")

            # 按客户类型统计
            personal_count = sum(1 for row in results if row[0] == '个人客户')
            corporate_count = sum(1 for row in results if row[0] == '企业客户')
            personal_amount = sum(row[4] for row in results if row[0] == '个人客户')
            corporate_amount = sum(row[4] for row in results if row[0] == '企业客户')

            print(f"\n[按客户类型统计]")
            print("-" * 50)
            print(f"个人客户: {personal_count}个, 报告金额: {personal_amount:,.2f} 元")
            print(f"企业客户: {corporate_count}个, 报告金额: {corporate_amount:,.2f} 元")

        else:
            print("未找到匹配的大额交易报告记录")

        cursor.close()
        conn.close()

        print(f"\n[SUCCESS] 大额交易报告分析完成！")
        return execution_time, len(results)

    except Exception as e:
        print(f"[ERROR] 查询失败: {e}")
        return 0, 0

def main():
    """主函数"""
    print("[INFO] 开始执行高风险客户大额交易报告统计...")

    execution_time, record_count = execute_high_risk_lar_analysis()

    if execution_time > 0:
        print(f"\n[SQL执行性能分析]")
        print(f"执行时间: {execution_time:.2f} 毫秒")

        if execution_time < 50:
            performance_rating = "5星 优秀"
        elif execution_time < 100:
            performance_rating = "4星 良好"
        elif execution_time < 200:
            performance_rating = "3星 一般"
        else:
            performance_rating = "2星 需要优化"

        print(f"性能评级: {performance_rating}")

        print("\n[SUCCESS] 查询执行成功！")
        return 0
    else:
        print("\n[ERROR] 查询执行失败！")
        return 1

if __name__ == "__main__":
    exit(main())
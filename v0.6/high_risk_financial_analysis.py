#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高风险客户金融业务综合分析
查询高风险客户的存款金额、交易金额、交易笔数、贷款金额、账户数量等
"""

import mysql.connector
import sys

def high_risk_financial_analysis():
    """高风险客户金融业务综合分析"""
    print("="*80)
    print("高风险客户金融业务综合分析报告")
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

        # 1. 高风险客户账户数量统计
        print("\n[1] 高风险客户账户数量统计")
        print("-" * 80)

        cursor.execute("""
            SELECT
                CASE
                    WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                    WHEN u.Cst_no IS NOT NULL THEN '企业客户'
                END AS 客户类型,
                r.Cst_no AS 客户号,
                COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
                COUNT(a.Self_acc_no) AS 账户数量,
                SUM(CASE WHEN a.Acc_state = '1' THEN 1 ELSE 0 END) AS 正常账户,
                SUM(CASE WHEN a.Acc_state = '2' THEN 1 ELSE 0 END) AS 冻结账户,
                SUM(CASE WHEN a.Acc_state = '3' THEN 1 ELSE 0 END) AS 注销账户,
                r.Time AS 风险评估时间
            FROM tb_risk_new r
            LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
            LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
            LEFT JOIN tb_acc a ON r.Cst_no = a.Cst_no
            WHERE r.Risk_code = '01'
            GROUP BY r.Cst_no, p.Acc_name, u.Acc_name, r.Time,
                     CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
            ORDER BY COUNT(a.Self_acc_no) DESC
            LIMIT 15
        """)

        account_stats = cursor.fetchall()

        print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<15} {'账户数':<6} {'正常':<4} {'冻结':<4} {'注销':<4} {'评估时间':<8}")
        print("-" * 80)

        total_accounts = 0
        for row in account_stats:
            customer_type, cst_no, name, acc_count, normal, frozen, closed, risk_time = row
            total_accounts += acc_count
            print(f"{customer_type:<8} {cst_no:<10} {name[:15]:<15} {acc_count:<6} {normal:<4} {frozen:<4} {closed:<4} {risk_time:<8}")

        print(f"\n高风险客户账户总数: {total_accounts} 个")

        # 2. 高风险客户存款余额分析
        print("\n[2] 高风险客户存款余额分析（前10名）")
        print("-" * 80)

        cursor.execute("""
            SELECT
                CASE
                    WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                    WHEN u.Cst_no IS NOT NULL THEN '企业客户'
                END AS 客户类型,
                r.Cst_no AS 客户号,
                COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
                COALESCE(SUM(a.Bal), 0) AS 总存款余额,
                COUNT(a.Self_acc_no) AS 账户数量,
                COALESCE(SUM(a.Bal) / NULLIF(COUNT(a.Self_acc_no), 0), 0) AS 平均余额,
                MAX(a.Bal) AS 最高单户余额
            FROM tb_risk_new r
            LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
            LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
            LEFT JOIN tb_acc a ON r.Cst_no = a.Cst_no
            WHERE r.Risk_code = '01'
            GROUP BY r.Cst_no, p.Acc_name, u.Acc_name,
                     CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
            HAVING COALESCE(SUM(a.Bal), 0) > 0
            ORDER BY COALESCE(SUM(a.Bal), 0) DESC
            LIMIT 10
        """)

        deposit_stats = cursor.fetchall()

        print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<15} {'总余额(元)':<12} {'账户数':<6} {'平均余额':<10} {'最高余额':<10}")
        print("-" * 80)

        total_deposit = 0
        for row in deposit_stats:
            customer_type, cst_no, name, total_bal, acc_count, avg_bal, max_bal = row
            total_deposit += total_bal
            print(f"{customer_type:<8} {cst_no:<10} {name[:15]:<15} {total_bal:<12.2f} {acc_count:<6} {avg_bal:<10.2f} {max_bal:<10.2f}")

        print(f"\n前10名高风险客户总存款余额: {total_deposit:,.2f} 元")

        # 3. 高风险客户交易统计分析
        print("\n[3] 高风险客户交易统计分析（前10名）")
        print("-" * 80)

        cursor.execute("""
            SELECT
                CASE
                    WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                    WHEN u.Cst_no IS NOT NULL THEN '企业客户'
                END AS 客户类型,
                r.Cst_no AS 客户号,
                COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
                COUNT(t.Ticd) AS 交易笔数,
                COALESCE(SUM(t.Org_amt), 0) AS 交易总金额,
                COALESCE(AVG(t.Org_amt), 0) AS 平均交易金额,
                COALESCE(MAX(t.Org_amt), 0) AS 最大单笔交易,
                COALESCE(MIN(t.Org_amt), 0) AS 最小单笔交易
            FROM tb_risk_new r
            LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
            LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
            LEFT JOIN tb_acc_txn t ON r.Cst_no = t.Cst_no
            WHERE r.Risk_code = '01'
              AND t.Date BETWEEN '20200101' AND '20200630'
            GROUP BY r.Cst_no, p.Acc_name, u.Acc_name,
                     CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
            HAVING COUNT(t.Ticd) > 0
            ORDER BY COALESCE(SUM(t.Org_amt), 0) DESC
            LIMIT 10
        """)

        transaction_stats = cursor.fetchall()

        print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<15} {'交易笔数':<8} {'总金额(元)':<12} {'平均金额':<10} {'最大金额':<10}")
        print("-" * 80)

        total_transaction = 0
        for row in transaction_stats:
            customer_type, cst_no, name, txn_count, total_amt, avg_amt, max_amt, min_amt = row
            total_transaction += total_amt
            print(f"{customer_type:<8} {cst_no:<10} {name[:15]:<15} {txn_count:<8} {total_amt:<12.2f} {avg_amt:<10.2f} {max_amt:<10.2f}")

        print(f"\n前10名高风险客户交易总金额: {total_transaction:,.2f} 元")

        # 4. 高风险客户跨境交易分析
        print("\n[4] 高风险客户跨境交易分析")
        print("-" * 80)

        cursor.execute("""
            SELECT
                CASE
                    WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                    WHEN u.Cst_no IS NOT NULL THEN '企业客户'
                END AS 客户类型,
                r.Cst_no AS 客户号,
                COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
                COUNT(cb.Ticd) AS 跨境交易笔数,
                COALESCE(SUM(cb.Org_amt), 0) AS 跨境交易总金额,
                COALESCE(SUM(cb.Rmb_amt), 0) as 人民币总金额,
                COUNT(DISTINCT cb.Part_nation) AS 涉及国家数量
            FROM tb_risk_new r
            LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
            LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
            LEFT JOIN tb_cross_border cb ON r.Cst_no = cb.Cst_no
            WHERE r.Risk_code = '01'
              AND cb.Date BETWEEN '20200101' AND '20200630'
            GROUP BY r.Cst_no, p.Acc_name, u.Acc_name,
                     CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
            HAVING COUNT(cb.Ticd) > 0
            ORDER BY COALESCE(SUM(cb.Org_amt), 0) DESC
        """)

        cross_border_stats = cursor.fetchall()

        if cross_border_stats:
            print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<15} {'跨境笔数':<8} {'跨境总金额':<12} {'人民币金额':<12} {'涉及国家':<8}")
            print("-" * 80)

            total_cross_border = 0
            for row in cross_border_stats:
                customer_type, cst_no, name, cb_count, cb_total, rmb_total, countries = row
                total_cross_border += rmb_total
                print(f"{customer_type:<8} {cst_no:<10} {name[:15]:<15} {cb_count:<8} {cb_total:<12.2f} {rmb_total:<12.2f} {countries:<8}")

            print(f"\n高风险客户跨境交易总金额: {total_cross_border:,.2f} 元")
        else:
            print("高风险客户无跨境交易记录")

        # 5. 高风险客户大额交易报告统计
        print("\n[5] 高风险客户大额交易报告统计")
        print("-" * 80)

        cursor.execute("""
            SELECT
                CASE
                    WHEN p.Cst_no IS NOT NULL THEN '个人客户'
                    WHEN u.Cst_no IS NOT NULL THEN '企业客户'
                END AS 客户类型,
                r.Cst_no AS 客户号,
                COALESCE(p.Acc_name, u.Acc_name) AS 客户名称,
                COUNT(lar.RPMN) AS 大额交易报告数量,
                COALESCE(SUM(lar.Report_Amount), 0) AS 报告总金额,
                MAX(lar.Report_Amount) AS 最大报告金额,
                COUNT(DISTINCT lar.Report_Date) AS 报告日期数量
            FROM tb_risk_new r
            LEFT JOIN tb_cst_pers p ON r.Cst_no = p.Cst_no AND r.Acc_type = '11'
            LEFT JOIN tb_cst_unit u ON r.Cst_no = u.Cst_no AND r.Acc_type = '13'
            LEFT JOIN tb_lar_report lar ON
                (lar.Customer_Name = p.Acc_name OR lar.Customer_Name = u.Acc_name)
            WHERE r.Risk_code = '01'
              AND lar.Report_Date BETWEEN '20200101' AND '20200630'
            GROUP BY r.Cst_no, p.Acc_name, u.Acc_name,
                     CASE WHEN p.Cst_no IS NOT NULL THEN '个人客户' ELSE '企业客户' END
            HAVING COUNT(lar.RPMN) > 0
            ORDER BY COUNT(lar.RPMN) DESC
        """)

        lar_stats = cursor.fetchall()

        if lar_stats:
            print(f"{'客户类型':<8} {'客户号':<10} {'客户名称':<15} {'报告数量':<8} {'报告总金额':<12} {'最大金额':<10}")
            print("-" * 80)

            total_lar_amount = 0
            for row in lar_stats:
                customer_type, cst_no, name, report_count, total_amount, max_amount, report_days = row
                total_lar_amount += total_amount
                print(f"{customer_type:<8} {cst_no:<10} {name[:15]:<15} {report_count:<8} {total_amount:<12.2f} {max_amount:<10.2f}")

            print(f"\n高风险客户大额交易报告总金额: {total_lar_amount:,.2f} 元")
        else:
            print("高风险客户无大额交易报告记录")

        # 6. 总体统计汇总
        print("\n[6] 高风险客户金融业务总体汇总")
        print("-" * 80)

        # 获取各项总体统计数据
        cursor.execute("SELECT COUNT(*) FROM tb_risk_new WHERE Risk_code = '01'")
        total_high_risk_customers = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(DISTINCT a.Self_acc_no)
            FROM tb_risk_new r
            JOIN tb_acc a ON r.Cst_no = a.Cst_no
            WHERE r.Risk_code = '01'
        """)
        total_high_risk_accounts = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(a.Bal), 0)
            FROM tb_risk_new r
            JOIN tb_acc a ON r.Cst_no = a.Cst_no
            WHERE r.Risk_code = '01'
        """)
        total_high_risk_balance = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COUNT(t.Ticd)
            FROM tb_risk_new r
            JOIN tb_acc_txn t ON r.Cst_no = t.Cst_no
            WHERE r.Risk_code = '01'
              AND t.Date BETWEEN '20200101' AND '20200630'
        """)
        total_high_risk_transactions = cursor.fetchone()[0]

        cursor.execute("""
            SELECT COALESCE(SUM(t.Org_amt), 0)
            FROM tb_risk_new r
            JOIN tb_acc_txn t ON r.Cst_no = t.Cst_no
            WHERE r.Risk_code = '01'
              AND t.Date BETWEEN '20200101' AND '20200630'
        """)
        total_high_risk_amount = cursor.fetchone()[0]

        print(f"高风险客户总数: {total_high_risk_customers}")
        print(f"高风险客户账户总数: {total_high_risk_accounts}")
        print(f"高风险客户总存款余额: {total_high_r_balance:,.2f} 元")
        print(f"高风险客户交易总笔数: {total_high_risk_transactions:,} 笔")
        print(f"高风险客户交易总金额: {total_high_r_amount:,.2f} 元")

        if total_high_risk_customers > 0:
            print(f"平均每个高风险客户账户数: {total_high_risk_accounts / total_high_risk_customers:.1f} 个")
            print(f"平均每个高风险客户存款余额: {total_high_r_balance / total_high_risk_customers:,.2f} 元")
            print(f"平均每个高风险客户交易笔数: {total_high_risk_transactions / total_high_risk_customers:.0f} 笔")
            print(f"平均每个高风险客户交易金额: {total_high_r_amount / total_high_risk_customers:,.2f} 元")

        cursor.close()
        conn.close()

        print("\n[SUCCESS] 高风险客户金融业务分析完成！")
        return True

    except Exception as e:
        print(f"[ERROR] 分析失败: {e}")
        return False

def main():
    """主函数"""
    print("[INFO] 开始高风险客户金融业务综合分析...")

    if high_risk_financial_analysis():
        print("\n[SUCCESS] 分析成功！")
        sys.exit(0)
    else:
        print("\n[ERROR] 分析失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()
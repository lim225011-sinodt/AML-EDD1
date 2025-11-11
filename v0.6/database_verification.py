#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于300号文校验规则对AML300数据库进行校验
检查15张表中的数据是否符合规范要求
"""

import mysql.connector
import re
import sys
from datetime import datetime, timedelta
from collections import defaultdict

class DatabaseVerifier:
    def __init__(self):
        self.conn = None
        self.cursor = None
        self.verification_results = {}
        self.error_count = 0
        self.warning_count = 0

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

    def get_table_list(self):
        """获取所有表名"""
        try:
            self.cursor.execute("SHOW TABLES")
            tables = [table[0] for table in self.cursor.fetchall()]
            print(f"[INFO] 数据库中共有 {len(tables)} 张表")
            return tables
        except Exception as e:
            print(f"[ERROR] 获取表列表失败: {e}")
            return []

    def verify_id_numbers(self):
        """校验证件号码"""
        print("\n=== 证件号码校验 ===")

        # 定义证件号码校验规则
        def validate_id_number(id_no, id_type):
            errors = []

            # 规则5：所有证件号码必须大于或等于6位
            if len(str(id_no)) < 6:
                errors.append("证件号码长度不足6位")

            # 规则2：身份证校验
            if id_type in ['11', '12']:  # 假设是身份证类型
                id_str = str(id_no)

                # 检查长度
                if len(id_str) not in [15, 18]:
                    errors.append(f"身份证号码长度应为15或18位，实际为{len(id_str)}位")

                # 检查是否为全数字或17位数字+X
                if len(id_str) == 18:
                    if not (id_str[:17].isdigit() and (id_str[17].isdigit() or id_str[17].upper() == 'X')):
                        errors.append("18位身份证号码前17位应为数字，最后位应为数字或X")
                elif len(id_str) == 15:
                    if not id_str.isdigit():
                        errors.append("15位身份证号码应全部为数字")

                # 检查前2位行政区划代码
                if len(id_str) >= 2:
                    if not id_str[:2].isdigit():
                        errors.append("身份证号码前2位应为数字（行政区划代码）")

                # 检查出生日期（18位）
                if len(id_str) == 18:
                    try:
                        year = int(id_str[6:10])
                        month = int(id_str[10:12])
                        day = int(id_str[12:14])

                        if not (1900 <= year <= 2025):
                            errors.append(f"身份证号码出生年份不合理：{year}")
                        if not (1 <= month <= 12):
                            errors.append(f"身份证号码出生月份不合理：{month}")
                        if not (1 <= day <= 31):
                            errors.append(f"身份证号码出生日期不合理：{day}")
                    except ValueError:
                        errors.append("身份证号码出生日期格式错误")

            # 规则7：括号配对检查
            if isinstance(id_no, str):
                open_brackets = id_no.count('(') + id_no.count('[') + id_no.count('{')
                close_brackets = id_no.count(')') + id_no.count(']') + id_no.count('}')
                if open_brackets != close_brackets:
                    errors.append("括号不匹配")

            # 规则6：全角字符检查
            if isinstance(id_no, str):
                # 检查是否有全角字符
                if any(ord(char) > 127 and not char.isalpha() for char in id_no):
                    errors.append("证件号码包含全角字符")

            return errors

        # 校验个人客户证件号码
        try:
            self.cursor.execute("""
                SELECT Cst_no, Id_no, Id_type
                FROM tb_cst_pers
                LIMIT 100
            """)

            personal_records = self.cursor.fetchall()
            personal_errors = 0

            for cst_no, id_no, id_type in personal_records:
                if id_no:
                    errors = validate_id_number(id_no, id_type)
                    if errors:
                        personal_errors += 1
                        self.error_count += len(errors)
                        if personal_errors <= 5:  # 只显示前5个错误详情
                            print(f"  [ERROR] 个人客户 {cst_no} 证件号码 {id_no}: {', '.join(errors)}")

            print(f"个人客户证件号码校验：{len(personal_records)}条记录，{personal_errors}条有错误")

            # 校验企业客户证件号码
            self.cursor.execute("""
                SELECT Cst_no, License
                FROM tb_cst_unit
                LIMIT 50
            """)

            corporate_records = self.cursor.fetchall()
            corporate_errors = 0

            for cst_no, license_no in corporate_records:
                if license_no:
                    # 统一社会信用代码校验（18位）
                    if len(str(license_no)) == 18:
                        # 前17位应为数字，第18位可以是数字或字母
                        license_str = str(license_no)
                        if not (license_str[:17].isdigit() and (license_str[17].isdigit() or license_str[17].upper() in 'ABCDEFGHJKLMNPQRTUWXY')):
                            corporate_errors += 1
                            self.error_count += 1
                            if corporate_errors <= 3:
                                print(f"  [ERROR] 企业客户 {cst_no} 营业执照 {license_no}: 统一社会信用代码格式错误")
                    else:
                        corporate_errors += 1
                        self.error_count += 1
                        if corporate_errors <= 3:
                            print(f"  [ERROR] 企业客户 {cst_no} 营业执照 {license_no}: 统一社会信用代码长度不足18位")

            print(f"企业客户证件号码校验：{len(corporate_records)}条记录，{corporate_errors}条有错误")

        except Exception as e:
            print(f"[ERROR] 证件号码校验失败: {e}")

    def verify_account_numbers(self):
        """校验账号"""
        print("\n=== 账号校验 ===")

        try:
            self.cursor.execute("""
                SELECT Self_acc_no, Cst_no
                FROM tb_acc
                LIMIT 100
            """)

            account_records = self.cursor.fetchall()
            account_errors = 0

            for acc_no, cst_no in account_records:
                errors = []
                acc_str = str(acc_no)

                # 规则11：账号中不得含有除数字、字母、"-"之外的字符
                allowed_chars = set('0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ-')
                invalid_chars = [char for char in acc_str.upper() if char not in allowed_chars]
                if invalid_chars:
                    errors.append(f"账号包含非法字符: {invalid_chars}")

                # 规则12：账号不得全部为连续相同的数字
                if len(acc_str) > 1 and len(set(acc_str)) == 1:
                    errors.append("账号全部为相同数字")

                # 规则12：账号不得只填写一位数字
                if len(acc_str) == 1 and acc_str.isdigit():
                    errors.append("账号只包含一位数字")

                # 规则10：账号长度检查（虽然规则说>=6位，但实际银行账号通常较长）
                if len(acc_str) < 6:
                    errors.append("账号长度不足6位")

                if errors:
                    account_errors += 1
                    self.error_count += len(errors)
                    if account_errors <= 5:
                        print(f"  [ERROR] 客户 {cst_no} 账号 {acc_no}: {', '.join(errors)}")

            print(f"账号校验：{len(account_records)}条记录，{account_errors}条有错误")

        except Exception as e:
            print(f"[ERROR] 账号校验失败: {e}")

    def verify_bank_codes(self):
        """校验网点代码"""
        print("\n=== 网点代码校验 ===")

        try:
            # 获取银行网点代码
            self.cursor.execute("SELECT DISTINCT Bank_code1 FROM tb_bank")
            bank_codes = self.cursor.fetchall()

            bank_code_errors = 0
            valid_patterns = [
                r'^\d{12}$',  # 12位数字（金融机构代码）
                r'^\d{6}$',   # 6位数字（银行内部机构号）
            ]

            for (bank_code,) in bank_codes:
                if not bank_code:
                    bank_code_errors += 1
                    self.error_count += 1
                    print(f"  [ERROR] 空的网点代码")
                    continue

                # 检查是否为数字
                if not bank_code.isdigit():
                    bank_code_errors += 1
                    self.error_count += 1
                    if bank_code_errors <= 3:
                        print(f"  [ERROR] 网点代码 {bank_code}: 包含非数字字符")
                    continue

                # 检查长度是否符合规范
                if len(bank_code) not in [6, 12]:
                    bank_code_errors += 1
                    self.error_count += 1
                    if bank_code_errors <= 3:
                        print(f"  [ERROR] 网点代码 {bank_code}: 长度不符合规范（应为6或12位）")

            print(f"网点代码校验：{len(bank_codes)}个代码，{bank_code_errors}个有错误")

        except Exception as e:
            print(f"[ERROR] 网点代码校验失败: {e}")

    def verify_dates_and_times(self):
        """校验日期和时间"""
        print("\n=== 日期和时间校验 ===")

        try:
            # 校验日期格式
            date_fields = [
                ('tb_cst_pers', ['']),
                ('tb_cst_unit', ['']),
                ('tb_acc', ['Open_time', 'Close_time']),
                ('tb_acc_txn', ['Date']),
                ('tb_risk_new', ['Time']),
                ('tb_lar_report', ['Report_Date', 'Transaction_Date']),
                ('tb_sus_report', ['Report_Date'])
            ]

            date_errors = 0

            for table, date_columns in date_fields:
                try:
                    for col in date_columns:
                        if col:
                            self.cursor.execute(f"SELECT {col} FROM {table} WHERE {col} IS NOT NULL LIMIT 10")
                            dates = self.cursor.fetchall()

                            for (date_str,) in dates:
                                if date_str:
                                    # 检查长度和格式
                                    if len(date_str) == 8 and date_str.isdigit():
                                        year = int(date_str[:4])
                                        month = int(date_str[4:6])
                                        day = int(date_str[6:8])

                                        if not (1900 <= year <= 2030):
                                            date_errors += 1
                                            if date_errors <= 3:
                                                print(f"  [ERROR] {table}.{col}: 年份不合理 {year}")

                                        if not (1 <= month <= 12):
                                            date_errors += 1
                                            if date_errors <= 3:
                                                print(f"  [ERROR] {table}.{col}: 月份不合理 {month}")

                                        if not (1 <= day <= 31):
                                            date_errors += 1
                                            if date_errors <= 3:
                                                print(f"  [ERROR] {table}.{col}: 日期不合理 {day}")
                                    else:
                                        date_errors += 1
                                        if date_errors <= 3:
                                            print(f"  [ERROR] {table}.{col}: 日期格式错误 {date_str}")

                except Exception as e:
                    print(f"  [WARNING] 无法校验 {table}.{col}: {e}")

            print(f"日期时间校验：{date_errors}个错误")

        except Exception as e:
            print(f"[ERROR] 日期时间校验失败: {e}")

    def verify_names(self):
        """校验客户名称"""
        print("\n=== 客户名称校验 ===")

        try:
            # 校验个人客户名称
            self.cursor.execute("""
                SELECT Acc_name, Cst_no
                FROM tb_cst_pers
                LIMIT 50
            """)

            personal_names = self.cursor.fetchall()
            name_errors = 0

            for name, cst_no in personal_names:
                if name:
                    # 规则25：客户名称不能仅由数字组成
                    if name.isdigit():
                        name_errors += 1
                        self.error_count += 1
                        if name_errors <= 3:
                            print(f"  [ERROR] 个人客户 {cst_no} 名称 '{name}': 仅由数字组成")

                    # 规则26：长度不得少于2个字符
                    if len(name.strip()) < 2:
                        name_errors += 1
                        self.error_count += 1
                        if name_errors <= 3:
                            print(f"  [ERROR] 个人客户 {cst_no} 名称 '{name}': 长度不足2个字符")

            # 校验企业客户名称
            self.cursor.execute("""
                SELECT Acc_name, Cst_no
                FROM tb_cst_unit
                LIMIT 20
            """)

            corporate_names = self.cursor.fetchall()
            corporate_name_errors = 0

            for name, cst_no in corporate_names:
                if name:
                    # 规则26：企业客户名称长度不得少于4个字符
                    if len(name.strip()) < 4:
                        corporate_name_errors += 1
                        self.error_count += 1
                        if corporate_name_errors <= 2:
                            print(f"  [ERROR] 企业客户 {cst_no} 名称 '{name}': 长度不足4个字符")

            print(f"个人客户名称校验：{len(personal_names)}条记录，{name_errors}条有错误")
            print(f"企业客户名称校验：{len(corporate_names)}条记录，{corporate_name_errors}条有错误")

        except Exception as e:
            print(f"[ERROR] 客户名称校验失败: {e}")

    def verify_amounts_and_currency(self):
        """校验交易金额和币种"""
        print("\n=== 交易金额和币种校验 ===")

        try:
            # 校验交易金额
            self.cursor.execute("""
                SELECT Org_amt, Ticd
                FROM tb_acc_txn
                WHERE Org_amt IS NOT NULL
                LIMIT 20
            """)

            transactions = self.cursor.fetchall()
            amount_errors = 0

            for amount, ticd in transactions:
                if amount is None:
                    amount_errors += 1
                    self.error_count += 1
                    if amount_errors <= 3:
                        print(f"  [ERROR] 交易 {ticd}: 交易金额为空")
                elif amount < 0:
                    amount_errors += 1
                    self.error_count += 1
                    if amount_errors <= 3:
                        print(f"  [ERROR] 交易 {ticd}: 交易金额为负数 {amount}")

            print(f"交易金额校验：{len(transactions)}条记录，{amount_errors}条有错误")

            # 校验币种代码
            self.cursor.execute("""
                SELECT DISTINCT Cur
                FROM tb_acc_txn
                WHERE Cur IS NOT NULL
            """)

            currencies = self.cursor.fetchall()
            valid_currencies = ['CNY', 'USD', 'EUR', 'GBP', 'JPY', 'HKD']
            currency_errors = 0

            for (currency,) in currencies:
                if currency not in valid_currencies:
                    currency_errors += 1
                    self.warning_count += 1
                    if currency_errors <= 3:
                        print(f"  [WARNING] 发现未知币种: {currency}")

            print(f"币种校验：{len(currencies)}种币种，{currency_errors}个未知币种")

        except Exception as e:
            print(f"[ERROR] 交易金额和币种校验失败: {e}")

    def verify_large_amount_transactions(self):
        """校验大额交易特征值"""
        print("\n=== 大额交易特征值校验 ===")

        try:
            # 获取大额交易报告
            self.cursor.execute("""
                SELECT Report_Amount, Currency, RPMN
                FROM tb_lar_report
                WHERE Report_Amount IS NOT NULL
                LIMIT 20
            """)

            lar_reports = self.cursor.fetchall()
            lar_errors = 0

            for amount, currency, ticd in lar_reports:
                # 大额交易特征：人民币5万元以上或外币等值1万美元以上
                if currency == 'CNY' and amount < 50000:
                    lar_errors += 1
                    self.warning_count += 1
                    if lar_errors <= 3:
                        print(f"  [WARNING] 大额报告 {rpmn}: 人民币金额 {amount} 低于5万元标准")
                elif currency != 'CNY' and amount < 10000:
                    lar_errors += 1
                    self.warning_count += 1
                    if lar_errors <= 3:
                        print(f"  [WARNING] 大额报告 {rpmn}: 外币金额 {amount} 可能低于1万美元标准")

            print(f"大额交易特征校验：{len(lar_reports)}条报告，{lar_errors}条不符合特征值要求")

        except Exception as e:
            print(f"[ERROR] 大额交易特征值校验失败: {e}")

    def verify_special_characters(self):
        """校验特殊字符"""
        print("\n=== 特殊字符校验 ===")

        special_chars = ['？', '！', '$', '%', '^', '*', '?', '!']

        try:
            # 检查各表的文本字段
            text_fields = [
                ('tb_cst_pers', ['Acc_name']),
                ('tb_cst_unit', ['Acc_name', 'Rep_name'])
            ]

            special_char_errors = 0

            for table, fields in text_fields:
                for field in fields:
                    try:
                        if table == 'tb_cst_pers':
                            self.cursor.execute(f"SELECT {field}, Cst_no FROM {table} WHERE {field} IS NOT NULL LIMIT 20")
                        else:
                            self.cursor.execute(f"SELECT {field}, Cst_no FROM {table} WHERE {field} IS NOT NULL LIMIT 20")
                        records = self.cursor.fetchall()

                        for text, cst_no in records:
                            if text:
                                for char in special_chars:
                                    if char in str(text):
                                        special_char_errors += 1
                                        self.error_count += 1
                                        if special_char_errors <= 3:
                                            print(f"  [ERROR] {table}.{field} 客户{cst_no}: 包含特殊字符 '{char}'")
                                        break  # 跳出字符循环
                                if special_char_errors <= 3:
                                    break  # 跳出记录循环

                    except Exception as e:
                        print(f"  [WARNING] 无法检查 {table}.{field}: {e}")

            print(f"特殊字符校验：发现 {special_char_errors} 个字段包含特殊字符")

        except Exception as e:
            print(f"[ERROR] 特殊字符校验失败: {e}")

    def run_verification(self):
        """执行完整校验流程"""
        print("=" * 80)
        print("AML300数据库300号文校验规则校验")
        print("=" * 80)

        try:
            if not self.connect_database():
                return False

            tables = self.get_table_list()
            print(f"发现表数量: {len(tables)}")
            print(f"目标表数量: 15")

            # 执行各项校验
            self.verify_id_numbers()
            self.verify_account_numbers()
            self.verify_bank_codes()
            self.verify_dates_and_times()
            self.verify_names()
            self.verify_amounts_and_currency()
            self.verify_large_amount_transactions()
            self.verify_special_characters()

            # 输出校验总结
            print("\n" + "=" * 80)
            print("校验结果总结")
            print("=" * 80)
            print(f"总错误数量: {self.error_count}")
            print(f"警告数量: {self.warning_count}")

            if self.error_count == 0:
                print("[OK] 数据质量优秀：完全符合300号文校验规则")
            elif self.error_count < 10:
                print("[WARNING] 数据质量良好：存在少量问题，建议修正")
            elif self.error_count < 50:
                print("[ERROR] 数据质量一般：存在较多问题，需要重点修正")
            else:
                print("[CRITICAL] 数据质量差：存在大量问题，必须立即修正")

            return True

        except Exception as e:
            print(f"[ERROR] 校验过程中发生错误: {e}")
            return False

        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()

def main():
    """主函数"""
    verifier = DatabaseVerifier()
    success = verifier.run_verification()

    if success:
        print("\n[SUCCESS] 数据库校验完成！")
        sys.exit(0)
    else:
        print("\n[ERROR] 数据库校验失败！")
        sys.exit(1)

if __name__ == "__main__":
    main()
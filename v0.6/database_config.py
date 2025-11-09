#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AML-EDD数据库配置文件
版本: v1.0
创建时间: 2025-11-09
功能: 数据库连接配置和测试参数
"""

import os

class DatabaseConfig:
    """数据库配置类"""

    # 默认数据库配置
    DEFAULT_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'root',
        'password': '',
        'database': 'test_aml_edd_v06',
        'charset': 'utf8mb4'
    }

    # 生产环境配置（可根据实际环境修改）
    PRODUCTION_CONFIG = {
        'host': os.environ.get('DB_HOST', 'localhost'),
        'port': int(os.environ.get('DB_PORT', 3306)),
        'user': os.environ.get('DB_USER', 'root'),
        'password': os.environ.get('DB_PASSWORD', ''),
        'database': os.environ.get('DB_NAME', 'aml_edd_v06'),
        'charset': 'utf8mb4'
    }

    # 测试环境配置
    TEST_CONFIG = {
        'host': 'localhost',
        'port': 3306,
        'user': 'test_user',
        'password': 'test_password',
        'database': 'test_aml_edd',
        'charset': 'utf8mb4'
    }

    @classmethod
    def get_config(cls, environment='default'):
        """获取指定环境的配置"""
        if environment == 'production':
            return cls.PRODUCTION_CONFIG
        elif environment == 'test':
            return cls.TEST_CONFIG
        else:
            return cls.DEFAULT_CONFIG

    @classmethod
    def test_connection(cls, config):
        """测试数据库连接"""
        try:
            import mysql.connector
            conn = mysql.connector.connect(**config)
            conn.close()
            return True
        except Exception as e:
            print(f"数据库连接测试失败: {e}")
            return False

# 数据验证配置
class ValidationConfig:
    """数据验证配置"""

    # 预期数据量范围
    EXPECTED_DATA_RANGES = {
        'tb_cst_pers': {'min': 900, 'max': 1100, 'name': '个人客户'},
        'tb_cst_unit': {'min': 90, 'max': 110, 'name': '企业客户'},
        'tb_acc': {'min': 800, 'max': 1200, 'name': '账户'},
        'tb_acc_txn': {'min': 9000, 'max': 11000, 'name': '交易记录'},
        'tb_risk_new': {'min': 1000, 'max': 1200, 'name': '最新风险等级'},
        'tb_risk_his': {'min': 300, 'max': 600, 'name': '历史风险等级'},
        'tb_bank': {'min': 8, 'max': 15, 'name': '机构信息'},
        'tb_settle_type': {'min': 12, 'max': 18, 'name': '业务类型'}
    }

    # 数据质量检查阈值
    QUALITY_THRESHOLDS = {
        'max_invalid_records': 10,  # 最大无效记录数
        'min_valid_percentage': 95.0,  # 最小有效记录百分比
        'max_null_percentage': 5.0   # 最大空值百分比
    }

    # 性能要求
    PERFORMANCE_REQUIREMENTS = {
        'max_execution_time': 300,  # 最大执行时间（秒）
        'max_memory_usage': '1GB',  # 最大内存使用
        'min_insert_speed': 100      # 最小插入速度（记录/秒）
    }

if __name__ == "__main__":
    # 示例：测试不同环境的数据库连接
    environments = ['default', 'test', 'production']

    for env in environments:
        config = DatabaseConfig.get_config(env)
        print(f"\n测试 {env} 环境配置:")
        print(f"  主机: {config['host']}:{config['port']}")
        print(f"  数据库: {config['database']}")
        print(f"  字符集: {config['charset']}")

        if DatabaseConfig.test_connection(config):
            print(f"  ✓ 连接成功")
        else:
            print(f"  ✗ 连接失败")
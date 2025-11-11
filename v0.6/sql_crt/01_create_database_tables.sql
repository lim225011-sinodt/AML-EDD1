-- =====================================================
-- AML300数据库建表脚本
-- 基于300号文件接口规范
-- 创建时间: 2025-11-11
-- 版本: v1.0
-- =====================================================

-- 创建数据库
CREATE DATABASE IF NOT EXISTS AML300
CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE AML300;

-- =====================================================
-- 1. 个人客户表 (tb_cst_pers)
-- =====================================================
CREATE TABLE tb_cst_pers (
    Cst_no VARCHAR(20) PRIMARY KEY COMMENT '客户号-个人',
    Acc_name VARCHAR(100) NOT NULL COMMENT '客户姓名',
    Sex CHAR(1) COMMENT '性别 1-男 2-女',
    Birth_dt CHAR(8) COMMENT '出生日期 YYYYMMDD',
    Id_type CHAR(2) NOT NULL COMMENT '证件类型 11-身份证 12-临时身份证',
    Id_no VARCHAR(30) NOT NULL COMMENT '证件号码',
    Nation VARCHAR(10) COMMENT '民族',
    Occ_type VARCHAR(10) COMMENT '职业',
    Degree VARCHAR(10) COMMENT '学历',
    Phone VARCHAR(20) COMMENT '联系电话',
    Mobile VARCHAR(20) COMMENT '手机号码',
    Email VARCHAR(100) COMMENT '电子邮箱',
    Reg_addr VARCHAR(200) COMMENT '注册地址',
    Live_addr VARCHAR(200) COMMENT '居住地址',
    Work_unit VARCHAR(100) COMMENT '工作单位',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='个人客户信息表';

-- =====================================================
-- 2. 企业客户表 (tb_cst_unit)
-- =====================================================
CREATE TABLE tb_cst_unit (
    Cst_no VARCHAR(20) PRIMARY KEY COMMENT '客户号-企业',
    Acc_name VARCHAR(200) NOT NULL COMMENT '企业名称',
    Legal_name VARCHAR(100) COMMENT '法定代表人姓名',
    Legal_id_no VARCHAR(30) COMMENT '法定代表人证件号码',
    Legal_phone VARCHAR(20) COMMENT '联系电话',
    Reg_capital DECIMAL(20,2) COMMENT '注册资本',
    Est_dt CHAR(8) COMMENT '成立日期 YYYYMMDD',
    License VARCHAR(30) COMMENT '营业执照号/统一社会信用代码',
    Org_code VARCHAR(20) COMMENT '组织机构代码',
    Tax_no VARCHAR(30) COMMENT '税务登记号',
    Bus_scope VARCHAR(500) COMMENT '经营范围',
    Reg_addr VARCHAR(200) COMMENT '注册地址',
    Work_addr VARCHAR(200) COMMENT '经营地址',
    Phone VARCHAR(20) COMMENT '联系电话',
    Email VARCHAR(100) COMMENT '电子邮箱',
    Rep_name VARCHAR(100) COMMENT '联系人姓名',
    Rep_phone VARCHAR(20) COMMENT '联系人电话',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='企业客户信息表';

-- =====================================================
-- 3. 银行网点表 (tb_bank)
-- =====================================================
CREATE TABLE tb_bank (
    Bank_code1 VARCHAR(20) PRIMARY KEY COMMENT '网点代码',
    Bank_name VARCHAR(100) NOT NULL COMMENT '网点名称',
    Bank_type VARCHAR(10) COMMENT '网点类型',
    Addr VARCHAR(200) COMMENT '地址',
    Phone VARCHAR(20) COMMENT '联系电话',
    Manager VARCHAR(50) COMMENT '负责人',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='银行网点信息表';

-- =====================================================
-- 4. 账户信息表 (tb_acc)
-- =====================================================
CREATE TABLE tb_acc (
    Self_acc_no VARCHAR(30) PRIMARY KEY COMMENT '账号',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Acc_type CHAR(2) COMMENT '账户类型 11-个人 13-企业',
    Acc_name VARCHAR(200) NOT NULL COMMENT '账户名称',
    Acc_cur CHAR(3) DEFAULT 'CNY' COMMENT '账户币种',
    Acc_state CHAR(1) DEFAULT '1' COMMENT '账户状态 1-正常 2-冻结 3-注销',
    Open_time CHAR(8) COMMENT '开户日期 YYYYMMDD',
    Close_time CHAR(8) COMMENT '销户日期 YYYYMMDD',
    Open_bank VARCHAR(20) COMMENT '开户网点',
    Bus_type VARCHAR(10) COMMENT '业务类型',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Open_bank) REFERENCES tb_bank(Bank_code1),
    INDEX idx_cst_no (Cst_no),
    INDEX idx_acc_type (Acc_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账户信息表';

-- =====================================================
-- 5. 账户交易表 (tb_acc_txn)
-- =====================================================
CREATE TABLE tb_acc_txn (
    Ticd BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '交易流水号',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Self_acc_no VARCHAR(30) NOT NULL COMMENT '账号',
    Date CHAR(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
    Tstm CHAR(15) COMMENT '交易时间 HHMMSSffffff',
    Cur CHAR(3) DEFAULT 'CNY' COMMENT '交易币种',
    Org_amt DECIMAL(20,2) NOT NULL COMMENT '交易金额',
    Rmb_amt DECIMAL(20,2) COMMENT '人民币金额',
    Bal DECIMAL(20,2) COMMENT '账户余额',
    Opp_acc VARCHAR(30) COMMENT '对方账号',
    Opp_name VARCHAR(200) COMMENT '对方户名',
    Opp_bank VARCHAR(20) COMMENT '对方银行',
    Txn_type VARCHAR(10) COMMENT '交易类型',
    Txn_code VARCHAR(20) COMMENT '交易代码',
    Summary VARCHAR(200) COMMENT '交易摘要',
    Channel VARCHAR(10) COMMENT '交易渠道',
    Teller_id VARCHAR(20) COMMENT '柜员号',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Self_acc_no) REFERENCES tb_acc(Self_acc_no) ON DELETE CASCADE,
    INDEX idx_cst_date (Cst_no, Date),
    INDEX idx_acc_date (Self_acc_no, Date),
    INDEX idx_txn_date (Date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='账户交易流水表';

-- =====================================================
-- 6. 风险等级评估表 (tb_risk_new)
-- =====================================================
CREATE TABLE tb_risk_new (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Acc_type CHAR(2) COMMENT '账户类型 11-个人 13-企业',
    Risk_code CHAR(2) NOT NULL COMMENT '风险等级 01-高风险 02-中高风险 03-中风险 04-低风险',
    Risk_score DECIMAL(5,2) COMMENT '风险评分',
    Time CHAR(8) COMMENT '评估时间 YYYYMMDD',
    Risk_reason VARCHAR(500) COMMENT '风险原因',
    Evaluator VARCHAR(50) COMMENT '评估人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_risk_code (Risk_code),
    INDEX idx_cst_risk (Cst_no, Risk_code),
    INDEX idx_eval_time (Time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户风险等级评估表';

-- =====================================================
-- 7. 大额交易报告表 (tb_lar_report)
-- =====================================================
CREATE TABLE tb_lar_report (
    RPMN VARCHAR(50) PRIMARY KEY COMMENT '报告流水号',
    Report_Date CHAR(8) NOT NULL COMMENT '报告日期 YYYYMMDD',
    Institution_Code VARCHAR(20) COMMENT '报告机构代码',
    Customer_ID VARCHAR(20) COMMENT '客户号',
    Customer_Name VARCHAR(200) COMMENT '客户名称',
    Customer_Type CHAR(2) COMMENT '客户类型',
    Account_No VARCHAR(30) COMMENT '账号',
    Account_Name VARCHAR(200) COMMENT '账户名称',
    Transaction_Date CHAR(8) COMMENT '交易日期 YYYYMMDD',
    Transaction_Time CHAR(15) COMMENT '交易时间 HHMMSSffffff',
    Reporting_Feature_Code VARCHAR(10) COMMENT '大额交易特征代码',
    Report_Amount DECIMAL(20,2) NOT NULL COMMENT '报告金额',
    Currency CHAR(3) DEFAULT 'CNY' COMMENT '币种',
    Transaction_Type VARCHAR(10) COMMENT '交易类型',
    Counterpart_Account VARCHAR(30) COMMENT '交易对手账号',
    Counterpart_Name VARCHAR(200) COMMENT '交易对手名称',
    Counterpart_Bank VARCHAR(50) COMMENT '交易对手银行',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Customer_ID) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Customer_ID) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_report_date (Report_Date),
    INDEX idx_customer (Customer_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='大额交易报告表';

-- =====================================================
-- 8. 可疑交易报告表 (tb_sus_report)
-- =====================================================
CREATE TABLE tb_sus_report (
    SNO VARCHAR(50) PRIMARY KEY COMMENT '可疑报告号',
    Report_Date CHAR(8) NOT NULL COMMENT '报告日期 YYYYMMDD',
    Institution_Code VARCHAR(20) COMMENT '报告机构代码',
    Customer_ID VARCHAR(20) COMMENT '客户号',
    Customer_Name VARCHAR(200) COMMENT '客户名称',
    Customer_Type CHAR(2) COMMENT '客户类型',
    Account_No VARCHAR(30) COMMENT '账号',
    Account_Name VARCHAR(200) COMMENT '账户名称',
    Suspicious_Feature_Code VARCHAR(20) COMMENT '可疑交易特征代码',
    Transaction_Date CHAR(8) COMMENT '交易日期 YYYYMMDD',
    Transaction_Amount DECIMAL(20,2) COMMENT '交易金额',
    Currency CHAR(3) DEFAULT 'CNY' COMMENT '币种',
    Suspicious_Reason TEXT COMMENT '可疑原因描述',
    Report_Content TEXT COMMENT '报告内容',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Customer_ID) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Customer_ID) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_report_date (Report_Date),
    INDEX idx_customer (Customer_ID)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='可疑交易报告表';

-- =====================================================
-- 9. 跨境交易表 (tb_cross_border)
-- =====================================================
CREATE TABLE tb_cross_border (
    Ticd BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '交易流水号',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Self_acc_no VARCHAR(30) NOT NULL COMMENT '账号',
    Date CHAR(8) NOT NULL COMMENT '交易日期 YYYYMMDD',
    Cur CHAR(3) NOT NULL COMMENT '币种',
    Org_amt DECIMAL(20,2) NOT NULL COMMENT '原币金额',
    Rmb_amt DECIMAL(20,2) NOT NULL COMMENT '人民币金额',
    Exchange_rate DECIMAL(10,6) COMMENT '汇率',
    Part_nation VARCHAR(10) COMMENT '对方国家代码',
    Part_name VARCHAR(200) COMMENT '对方名称',
    Part_account VARCHAR(30) COMMENT '对方账号',
    Part_bank VARCHAR(100) COMMENT '对方银行',
    Purpose VARCHAR(200) COMMENT '交易目的',
    Txn_type VARCHAR(10) COMMENT '交易类型',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Self_acc_no) REFERENCES tb_acc(Self_acc_no) ON DELETE CASCADE,
    INDEX idx_cst_date (Cst_no, Date),
    INDEX idx_nation (Part_nation)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='跨境交易记录表';

-- =====================================================
-- 10. 联网核查日志表 (tb_lwhc_log)
-- =====================================================
CREATE TABLE tb_lwhc_log (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Date CHAR(8) NOT NULL COMMENT '核查日期 YYYYMMDD',
    Cst_no VARCHAR(20) COMMENT '客户号',
    Name VARCHAR(100) COMMENT '姓名/企业名称',
    Id_no VARCHAR(30) NOT NULL COMMENT '证件号码',
    Id_type CHAR(2) COMMENT '证件类型',
    Result CHAR(2) COMMENT '核查结果 11-一致且照片存在 12-一致无照片 13-一致照片错误 14-不匹配 15-不存在 16-其他',
    Operator VARCHAR(50) COMMENT '操作人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_id_no (Id_no),
    INDEX idx_result (Result),
    INDEX idx_check_date (Date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='联网核查记录表';

-- =====================================================
-- 11. 客户身份信息变更表 (tb_identity_change)
-- =====================================================
CREATE TABLE tb_identity_change (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Change_Date CHAR(8) NOT NULL COMMENT '变更日期 YYYYMMDD',
    Change_Type VARCHAR(20) COMMENT '变更类型',
    Old_Value VARCHAR(500) COMMENT '变更前值',
    New_Value VARCHAR(500) COMMENT '变更后值',
    Reason VARCHAR(200) COMMENT '变更原因',
    Approver VARCHAR(50) COMMENT '审批人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_cst_change_date (Cst_no, Change_Date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户身份信息变更表';

-- =====================================================
-- 12. 洗钱风险等级评估表 (tb_aml_risk_assessment)
-- =====================================================
CREATE TABLE tb_aml_risk_assessment (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Assessment_Date CHAR(8) NOT NULL COMMENT '评估日期 YYYYMMDD',
    Risk_Level CHAR(2) COMMENT '洗钱风险等级',
    Risk_Score DECIMAL(5,2) COMMENT '风险评分',
    Assessment_Factors TEXT COMMENT '评估因子',
    Assessment_Result TEXT COMMENT '评估结果',
    Assessor VARCHAR(50) COMMENT '评估人员',
    Review_Date CHAR(8) COMMENT '复核日期',
    Reviewer VARCHAR(50) COMMENT '复核人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_risk_level (Risk_Level),
    INDEX idx_assessment_date (Assessment_Date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='洗钱风险等级评估表';

-- =====================================================
-- 13. 政治公众人物表 (tb_pep_info)
-- =====================================================
CREATE TABLE tb_pep_info (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Pep_Type VARCHAR(10) COMMENT 'PEP类型',
    Pep_Name VARCHAR(200) COMMENT 'PEP姓名',
    Pep_Position VARCHAR(200) COMMENT 'PEP职务',
    Pep_Country VARCHAR(10) COMMENT 'PEP所在国家',
    Pep_Organization VARCHAR(200) COMMENT 'PEP所在机构',
    Relationship VARCHAR(100) COMMENT '与客户关系',
    Start_Date CHAR(8) COMMENT '开始日期 YYYYMMDD',
    End_Date CHAR(8) COMMENT '结束日期 YYYYMMDD',
    Status VARCHAR(10) COMMENT '状态',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_pep_type (Pep_Type),
    INDEX idx_pep_country (Pep_Country)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='政治公众人物信息表';

-- =====================================================
-- 14. 制裁名单匹配表 (tb_sanctions_screening)
-- =====================================================
CREATE TABLE tb_sanctions_screening (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Screen_Date CHAR(8) NOT NULL COMMENT '筛查日期 YYYYMMDD',
    Screen_Result VARCHAR(20) COMMENT '筛查结果',
    Matched_List VARCHAR(100) COMMENT '匹配名单',
    Match_Name VARCHAR(200) COMMENT '匹配姓名',
    Match_Score DECIMAL(5,2) COMMENT '匹配分数',
    Review_Result VARCHAR(20) COMMENT '复核结果',
    Reviewer VARCHAR(50) COMMENT '复核人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_screen_date (Screen_Date),
    INDEX idx_screen_result (Screen_Result)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='制裁名单筛查表';

-- =====================================================
-- 15. 客户尽职调查表 (tb_cdd_edd_record)
-- =====================================================
CREATE TABLE tb_cdd_edd_record (
    Id BIGINT PRIMARY KEY AUTO_INCREMENT COMMENT '主键ID',
    Cst_no VARCHAR(20) NOT NULL COMMENT '客户号',
    Investigation_Date CHAR(8) NOT NULL COMMENT '调查日期 YYYYMMDD',
    Investigation_Type VARCHAR(20) COMMENT '调查类型 CDD-客户尽职调查 EDD-增强尽职调查',
    Risk_Level CHAR(2) COMMENT '风险等级',
    Investigation_Content TEXT COMMENT '调查内容',
    Investigation_Result TEXT COMMENT '调查结果',
    Investigator VARCHAR(50) COMMENT '调查人员',
    Approval_Date CHAR(8) COMMENT '审批日期 YYYYMMDD',
    Approver VARCHAR(50) COMMENT '审批人员',
    Create_time DATETIME DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_pers(Cst_no) ON DELETE CASCADE,
    FOREIGN KEY (Cst_no) REFERENCES tb_cst_unit(Cst_no) ON DELETE CASCADE,
    INDEX idx_investigation_type (Investigation_Type),
    INDEX idx_investigation_date (Investigation_Date),
    INDEX idx_risk_level (Risk_Level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='客户尽职调查记录表';

-- =====================================================
-- 创建索引优化查询性能
-- =====================================================

-- 复合索引
CREATE INDEX idx_cst_acc_txn ON tb_acc_txn(Cst_no, Self_acc_no, Date);
CREATE INDEX idx_risk_customer ON tb_risk_new(Cst_no, Time, Risk_code);
CREATE INDEX idx_lar_customer ON tb_lar_report(Customer_ID, Report_Date);
CREATE INDEX idx_sus_customer ON tb_sus_report(Customer_ID, Report_Date);
CREATE INDEX idx_lwhc_customer ON tb_lwhc_log(Cst_no, Date);

-- 全文索引（如需要）
-- ALTER TABLE tb_cst_pers ADD FULLTEXT INDEX ft_name (Acc_name);
-- ALTER TABLE tb_cst_unit ADD FULLTEXT INDEX ft_name (Acc_name);

-- =====================================================
-- 建表完成
-- =====================================================

SELECT 'AML300数据库15张表创建完成！' AS Message;